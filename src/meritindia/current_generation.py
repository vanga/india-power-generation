import os
import requests
from pathlib import Path

import current_generation_helper as cgh

output_dir = Path("../../data/meritindia/current-generation/raw")
proxy_url = os.getenv("PROXY_URL")
# using a proxy since the meritindia API is only accessible from India IPs


req_body = {"type": "current_state_generation"}
res = requests.request(
    "POST",
    proxy_url,
    json=req_body,
    timeout=60,
)
try:
    rows = res.json()["data"]
except Exception:
    print(f"Failed to fetch data for {req_body}")
    print(res.text)
    raise Exception("Failed to fetch data")
cgh.save_data(rows, output_dir)
