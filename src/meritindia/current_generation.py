import os
import requests
from pathlib import Path

from meritindia import current_generation_helper as cgh

output_dir = Path("../../data/meritindia/current-generation/raw")
proxy_url = os.getenv("PROXY_URL")
# using a proxy since the meritindia API is only accessible from India IPs


req_body = {"type": "realtime_state_generation"}
res = requests.request("POST", proxy_url, data=req_body, timeout=60)
rows = res.json()["data"]
cgh.save_data(rows, output_dir)
