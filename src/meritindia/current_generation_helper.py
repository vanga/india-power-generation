import requests
import json
from pathlib import Path
from datetime import datetime
import pytz
import csv
import concurrent.futures
import lxml.html as LH

requests.packages.urllib3.disable_warnings()
# to disable the ssl verify warning

domain = "meritindia.in"
ip = "45.249.235.16"
url = f"https://{ip}/StateWiseDetails/BindCurrentStateStatus"
india_url = f"https://{ip}/Dashboard/BindAllIndiaMap"
# requesting domain some times resolves to an IP which is not respnosive. Looks like one of the servers behind the LB is not functional.
state_codes_path = Path("./state_codes.json")

timezone = "Asia/Kolkata"


def load_state_codes():
    with open(state_codes_path, "r") as file:
        state_codes = json.load(file)
    return state_codes


def request_current_data_india() -> str:
    response = requests.request(
        "GET", india_url, verify=False, timeout=60, headers={"Host": domain}
    )
    return response.text


def parse_india_data(data: str) -> dict:
    html_element = LH.fromstring(data)
    gen_values: dict[str] = html_element.xpath("//table")[1].xpath(
        ".//span[@class='counter']//text()"
    )
    gen_values = [item.strip(" \n\r").replace(",", "") for item in gen_values]
    headers = ["Demand", "Thermal", "GAS", "Nuclear", "Hydro", "Renewable"]
    row = dict(zip(headers, gen_values))
    return row


def get_india_row():
    india_data = request_current_data_india()
    india_row = parse_india_data(india_data)
    india_row["Datetime"] = datetime.now(pytz.timezone(timezone)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    india_row["StateCode"] = "IND"
    return india_row


def request_current_state_data(state_code):
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


def save_state_data(rows, dest_dir):
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


def save_india_data(row, dest_dir):
    dest_dir.mkdir(parents=True, exist_ok=True)
    file_name = "India-all"
    output_file = dest_dir / f"{file_name}.csv"

    headers = [
        "StateCode",
        "Datetime",
        "Demand",
        "Thermal",
        "GAS",
        "Nuclear",
        "Hydro",
        "Renewable",
    ]
    is_file_present = output_file.exists()
    with open(output_file, "a") as file:
        csv_writer = csv.DictWriter(file, fieldnames=headers)
        if not is_file_present:
            csv_writer.writeheader()

        csv_writer.writerow(row)
    print(f"Data written to {output_file}")


def get_data():
    state_codes = load_state_codes()
    rows = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for code, name in state_codes.items():
            futures.append(executor.submit(request_current_state_data, code))

        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            rows.append(row)
    return rows
