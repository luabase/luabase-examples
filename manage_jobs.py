import requests
import os
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

LUABASE_API_KEY = os.getenv('LUABASE_API_KEY')
INTEGRATION_ID = os.getenv('S3_INTEGRATION_ID')

job = {
        'ok': True, 
        'job_id': 'db1a9290461e4e03b2097854c9022b4c', 
        'block_id': 'fc4baeb505f9495697cfa95d6f61ddba'
        }

url = "https://q.luabase.com/manage"

payload = {
    "manage": {
        "action": "list_jobs",
        "block_uuid": job['block_id'],
        "format": "full_jobs"
    },
    "api_key": LUABASE_API_KEY,
}

headers = {"content-type": "application/json"}
response = requests.request("POST", url, json=payload, headers=headers)
data = response.json()
print(data)
df = pd.DataFrame(data['jobs'])
print(df)
# get a table with pd.DataFrame(data['jobs'])