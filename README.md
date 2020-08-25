# sample-ingestion-googlespreadsheet
Python for ingestion Google Spreadsheet to S3 Data Lake using AWS Data Wrangler

___

Steps

1 - Create a Google Spreadsheet

2 - Create the `credentials.json` file (using sample) and fill with your configurations

3 - Install Python Requirements
```bash
python -m pip install -U -r requirements.txt
```

4 - Execute `generate_token.py`. The `token.pickle` will be generated.

5 - Create the `local_config.json` file (using sample) and fill with your configurations

6 - Execute `sync_spreadsheet.py`
