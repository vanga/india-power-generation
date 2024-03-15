import os
import sys
import requests
from pathlib import Path

import current_generation_helper as cgh

output_dir = Path("../../data/meritindia/current-generation/raw")
proxy_url = os.getenv("PROXY_URL")
# using a proxy since the meritindia API is only accessible from India IPs

type = sys.argv[1]

if type == "states":
    req_body = {"type": "current-state-generation"}
    saver = cgh.save_state_data
elif type == "india":
    req_body = {"type": "current-india-generation"}
    saver = cgh.save_india_data

res = requests.request(
    "POST",
    proxy_url,
    json=req_body,
    timeout=60,
)
try:
    rows = res.json()["data"]
except Exception as e:
    print(f"Failed to fetch {type} data", e)
    print(res.text)
saver(rows, output_dir)
