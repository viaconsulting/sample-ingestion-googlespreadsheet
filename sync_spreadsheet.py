import json, os, pickle
from datetime import datetime

import boto3
import awswrangler as wr
import pandas as pd
from googleapiclient.discovery import build

###################################################################################################

dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__))) + os.sep

service = build(
    'sheets',
    'v4',
    credentials=pickle.load(
        open(dir+'token.pickle','rb')
    )
).spreadsheets()

###################################################################################################

def getSpreadsheetInfo(spreadsheetId):

    return service.get(spreadsheetId=spreadsheetId).execute()

###################################################################################################

def converter1toA(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

###################################################################################################

def getSheetValues(spreadsheetId,sheetRange):

    values = service.values().get(
        spreadsheetId=spreadsheetId,
        range=sheetRange
    ).execute().get('values',[])

    base = []
    if values != []:
        cols = values[0]
        for i in range(1,len(values)):
            row = {}
            for c in range(0,len(cols)):
                try:
                    row[cols[c]] = values[i][c]
                except:
                    row[cols[c]] = None
            base += [row]

    return base

###################################################################################################

def setSheetStatus(spreadsheetId,sheetTitle,statusCel,text):

    service.values().update(
        spreadsheetId=spreadsheetId,
        valueInputOption='RAW',
        range=f'{sheetTitle}!{statusCel}',
        body={'values':[[text]]}
    ).execute()

###################################################################################################

def checkDatabase(prefix,name):

    stackName = str(f'{prefix}{name}').replace('_','-')

    cf = boto3.client('cloudformation')

    try:
        cf.describe_stacks(
            StackName=stackName
        )
    except:
        cf.create_stack(
            StackName=stackName,
            TemplateBody=f'''
                AWSTemplateFormatVersion: 2010-09-09
                Resources:
                      Database:
                        Type: AWS::Glue::Database
                        Properties:
                            CatalogId: !Ref AWS::AccountId
                            DatabaseInput:
                                Name: '{name}'
            '''
        )

###################################################################################################

def main(bucket,env,id,prefix,status,start):

    # Get Spreadsheet Info
    info = getSpreadsheetInfo(id)

    # Check Database
    database = str(str(info['properties']['title']).replace(' ','_')+'_'+env).lower()
    checkDatabase(prefix,database)
    
    # Loop Sheets
    for sheet in info['sheets']:

        # Sheet Info
        title = sheet['properties']['title']
        rows  = sheet['properties']['gridProperties']['rowCount']
        cols  = sheet['properties']['gridProperties']['columnCount']

        # Status
        setSheetStatus(id,title,status,'Iniciando sincronização...')

        try:

            range = f'{title}!{start}:{converter1toA(cols)}{rows}'
            base = getSheetValues(id,range)

            if base != []:

                data = pd.DataFrame(base)

                table = title.replace(' ','_').lower()

                r = wr.s3.to_parquet(
                    df=data,
                    dataset=True,
                    mode='overwrite',
                    database=database,
                    table=table,
                    path=f's3://{bucket}/{database}/{table}/',
                    compression='snappy'
                )

            # Status
            setSheetStatus(id,title,status,f'''Sincronizado com sucesso: {datetime.now().strftime('%A, %d/%m/%Y às %H:%M')}''')

        except Exception as e:

            print('ERROR')
            print(str(e))

            # Status
            setSheetStatus(id,title,status,f'''ERRO {datetime.now().strftime('%A %d/%m/%Y às %H:%M')}: {e}''')

###################################################################################################
###################################################################################################

def handler(event):

    bucket = os.environ['BUCKET']
    env    = os.environ['ENVIROMENT']
    prefix = os.environ['CFSTACK_DATABASE_PREFIX']
    
    id     = event['id']
    status = event['status'] if 'status' in event else 'B1'
    start  = event['start'] if 'start' in event else 'A2'

    main(bucket,env,id,prefix,status,start)

###################################################################################################
### Local Tests
###################################################################################################

if __name__ == "__main__":

    config = json.load(open(dir+'local_config.json','r'))

    bucket = config['bucket']
    env    = config['env']
    id     = config['id']
    prefix = config['cfstack_database_prefix']
    status = config['status']
    start  = config['start']

    main(bucket,env,id,prefix,status,start)

###################################################################################################
