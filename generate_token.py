import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

tokenfile = 'token.pickle'
credfile = 'credentials.json'
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

print('start')

creds = None
if os.path.exists(tokenfile):
    print(tokenfile + ' exists')
    with open(tokenfile, 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        print('cred inválida')
        creds.refresh(Request())
        print('cred atualizada')
    else:
        print('cred não existe')
        print('abrindo ' + credfile)
        flow = InstalledAppFlow.from_client_secrets_file(
            credfile, SCOPES)
        creds = flow.run_local_server()
        print('cred gerada')
    
    with open(tokenfile, 'wb') as token:
        pickle.dump(creds, token)
    print('cred salva em ' + tokenfile)
else:
    print('cred válida')


print('end')
