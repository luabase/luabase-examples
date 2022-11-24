import requests
import os
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

LUABASE_API_KEY = os.getenv('LUABASE_API_KEY')
INTEGRATION_ID = os.getenv('S3_INTEGRATION_ID')

url = "https://q.luabase.com/operation"

job_name = "get_transactions"
bucket_name = "mjr-test-operations"

fd = open('get_transactions.sql', 'r')
sql = fd.read()
fd.close()

# sql = """
# select
# t.block_timestamp,
# t.block_number,
# t.block_hash,
# t.transaction_index,
# t.hash,
# t.from_address,
# t.to_address,
# t.value,
# t.gas,
# t.gas_price,
# t.input,
# t.nonce

# from ethereum.transactions as t
# where {{sync_date_param}} 
# """

payload = {
    "operation": {
        "type": "sql_sync",
        "name": "mjr_sync_date_transactions_nov11",
        "sql": sql,
        "destination": {
            "type": "s3",
            "fileName": "transactions_{{block_timestamp_start_ts}}",
            "if_exists": "create",
            "bucketName": bucket_name,
            "integration_id": INTEGRATION_ID,
            "format": "parquet",
            "no_extension": True
        },
        "schedule": {
            "period": "hourly",
        },
        "parameters": {
            "sync_date_param": {
                "type": "sync_dates", 
                "column": "block_timestamp",
                "start": "2022-11-01", 
                "granularity": "daily",
            },
        }
    },
    "api_key": LUABASE_API_KEY,
}

headers = {"content-type": "application/json"}
response = requests.request("POST", url, json=payload, headers=headers)
data = response.json()
print(data)