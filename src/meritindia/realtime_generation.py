import os
import requests
from pathlib import Path

from realtime_generation_helper import save_data

output_dir = Path("../../data/meritindia/raw")
proxy_url = os.getenv("PROXY_URL")
# using a proxy since the meritindia API is only accessible from India

res = requests.get(proxy_url, timeout=60)
rows = res.json()
save_data(rows, output_dir)
