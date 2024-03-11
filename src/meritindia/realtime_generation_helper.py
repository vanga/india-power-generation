import requests
import json
from pathlib import Path
from datetime import datetime
import pytz
import csv
import concurrent.futures

requests.packages.urllib3.disable_warnings()
# to disable the ssl verify warning

domain = "meritindia.in"
url = "https://45.249.235.16/StateWiseDetails/BindCurrentStateStatus"
# requesting domain some times resolves to an IP which is not respnosive. Looks like one of the servers behind the LB is not functional.
state_codes_path = Path("./state_codes.json")

timezone = "Asia/Kolkata"


def load_state_codes():
    with open(state_codes_path, "r") as file:
        state_codes = json.load(file)
    return state_codes


def get_realtime_data(state_code):
    response = requests.post(
        url,
        data={"StateCode": state_code},
        verify=False,
        headers={"Host": domain},
        timeout=60,
    )
    data = response.json()
    assert (
        len(data) == 1
    ), f"Unexpected data for state code: {state_code}, {json.dumps(data)}"
    row = data[0]
    row["Datetime"] = datetime.now(pytz.timezone(timezone)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    row["StateCode"] = state_code
    return row


def save_data(rows, dest_dir):
    dest_dir.mkdir(parents=True, exist_ok=True)
    file_name = datetime.now().strftime("%Y-%m")
    output_file = dest_dir / f"{file_name}.csv"

    headers = ["StateCode", "Datetime", "Demand", "ISGS", "ImportData"]
    is_file_present = output_file.exists()
    with open(output_file, "a") as file:
        csv_writer = csv.DictWriter(file, fieldnames=headers)
        if not is_file_present:
            csv_writer.writeheader()

        csv_writer.writerows(rows)
    print(f"Data written to {output_file}")


def run():
    state_codes = load_state_codes()
    rows = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for code, name in state_codes.items():
            futures.append(executor.submit(get_realtime_data, code))

        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            rows.append(row)
