import sys
import json
import os
import requests
from pathlib import Path

import daily_generation_helper as dgh

proxy_url = os.getenv("PROXY_URL")
# using a proxy since the meritindia API is only accessible from India IPs

data_type = sys.argv[1]

assert data_type in [
    "daily-state-generation",
    "daily-plant-generation",
], f"Unknown type: {data_type}"

request_inputs = list(dgh.get_request_inputs(data_type))
assert len(request_inputs) < 500, "Too many inputs"

if request_inputs:
    batch_size = 20
    num_batches = (len(request_inputs) + batch_size - 1) // batch_size

    for i in range(num_batches):
        batch_inputs = request_inputs[i * batch_size : (i + 1) * batch_size]
        req_body = {"type": data_type, "inputs": batch_inputs}
        print(json.dumps(req_body))
        res = requests.request("POST", proxy_url, json=req_body, timeout=120)
        try:
            rows = res.json()["data"]
        except Exception:
            print(f"Failed to fetch data for {req_body}")
            print(res.text)
            raise Exception("Failed to fetch data")
        dgh.save_data(data_type, rows)
        dgh.update_tracking_metadata(data_type, rows)
else:
    print("Nothing to get")
