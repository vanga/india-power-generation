import os
import requests
from pathlib import Path

import current_generation_helper as cgh

output_dir = Path("../../data/meritindia/current-generation/raw")
proxy_url = os.getenv("PROXY_URL")
# using a proxy since the meritindia API is only accessible from India IPs

state_failed = False
req_body = {"type": "current_state_generation"}
res = requests.request(
    "POST",
    proxy_url,
    json=req_body,
    timeout=60,
)
try:
    rows = res.json()["data"]
except Exception as e:
    print(f"Failed to fetch state data", e)
    print(res.text)
    state_failed = True
cgh.save_state_data(rows, output_dir)

india_res = requests.request(
    "POST",
    proxy_url,
    json={"type": "current_india_generation"},
    timeout=60,
)
try:
    india_row = india_res.json()["data"]
except Exception as e:
    print(f"Failed to fetch India data", e)
    print(india_res.text)
    raise e
cgh.save_india_data(india_row, output_dir)

if state_failed:
    raise Exception("Failed to fetch state data")
