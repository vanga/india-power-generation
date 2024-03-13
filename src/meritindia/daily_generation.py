import os
import requests
from pathlib import Path

import daily_generation_helper as dgh

output_dir = Path("../../data/meritindia/daily-generation/raw")
proxy_url = os.getenv("PROXY_URL")
# using a proxy since the meritindia API is only accessible from India IPs


request_inputs = list(dgh.get_request_inputs())
if request_inputs:
    req_body = {"type": "daily_state_generation", "inputs": request_inputs[:2]}
    res = requests.request("POST", proxy_url, json=req_body, timeout=60)
    try:
        rows = res.json()["data"]
    except Exception:
        print(f"Failed to fetch data for {req_body}")
        print(res.text)
        raise Exception("Failed to fetch data")
    dgh.save_data(rows, output_dir)
    dgh.save_tracking_data(rows)
else:
    print("Nothing to get")
