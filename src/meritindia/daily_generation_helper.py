import requests
import json
import csv
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import concurrent.futures
from typing import Iterable

requests.packages.urllib3.disable_warnings()
# to disable the ssl verify warning

domain = "meritindia.in"
url = "https://45.249.235.16/StateWiseDetails/GetStateWiseDetailsForPiChart"
timezone = "Asia/Kolkata"
starting_date = datetime(2017, 6, 1, tzinfo=pytz.timezone(timezone)).date()
tracking_path = "../../data/meritindia/track.json"
state_codes_path = "./state_codes.json"
output_dir = Path("../../data/meritindia/daily-generation/raw")
max_workers = 5


def load_tracking_data():
    if not Path(tracking_path).exists():
        return {}
    with open(tracking_path, "r") as file:
        tracking_data = json.load(file)
    return tracking_data


def load_state_codes() -> dict[str, str]:
    with open(state_codes_path, "r") as file:
        state_codes = json.load(file)
    return state_codes


def save_tracking_data(tracking_data: dict):
    with open(tracking_path, "w") as file:
        json.dump(tracking_data, file, indent=4)


def get_output_path(state_code):
    return Path(f"../../data/meritindia/daily-generation/raw/{state_code}.csv")


def get_rows_by_state(rows: list[dict]):
    rows_by_state = {}
    for row in rows:
        state_code = row["StateCode"]
        if state_code not in rows_by_state:
            rows_by_state[state_code] = []
        rows_by_state[state_code].append(row)
    return rows_by_state


def save_data(rows, dest_dir):
    headers = [
        "StateCode",
        "DateTime",
        "State Generation",
        "Central ISGS",
        "Other ISGS",
        "Bilateral",
        "Power Exchange",
        "fetched_at",
    ]
    dest_dir.mkdir(parents=True, exist_ok=True)
    # group rows by state code
    rows_by_state = get_rows_by_state(rows)
    for state_code, rows in rows_by_state.items():
        file_path = dest_dir / f"{state_code}.csv"
        is_file_present = file_path.exists()
        with open(file_path, "a") as file:
            csv_writer = csv.DictWriter(file, fieldnames=headers)
            if not is_file_present:
                csv_writer.writeheader()
            csv_writer.writerows(rows)
        print(f"{len(rows)} written for {state_code}")


def get_daily_state_generation(state_code, date: str):
    _data = datetime.strptime(date, "%Y-%m-%d").date()
    req_date_str = _data.strftime("%d %b %Y")
    res = requests.request(
        "POST",
        url,
        data={"StateCode": state_code, "date": req_date_str},
        verify=False,
        headers={"Host": domain},
    )
    data = res.json()
    row = {
        "fetched_at": datetime.now(tz=pytz.timezone(timezone)).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "StateCode": state_code,
        "DateTime": date,
    }
    for item in data:
        row[item["TypeOfEnergy"]] = item["EnergyValue"]

    return row


def get_request_inputs() -> Iterable[tuple[str, str]]:
    tracking_metadata = load_tracking_data()
    state_codes = load_state_codes()
    today = datetime.now(tz=pytz.timezone(timezone)).date()
    end_date = today - timedelta(days=2)
    for state_code in state_codes:
        if state_code not in tracking_metadata:
            start_date = starting_date
        else:
            start_date = (
                (
                    datetime.strptime(
                        tracking_metadata[state_code]["last_fetched"], "%Y-%m-%d"
                    )
                    + timedelta(days=1)
                )
                .astimezone(pytz.timezone(timezone))
                .date()
            )
        for day in range((end_date - start_date).days + 1):
            date = start_date + timedelta(days=day)
            yield state_code, str(date)


def get_data(request_inputs: list[tuple[str, str]]):
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for input in request_inputs:
            futures.append(executor.submit(get_daily_state_generation, *input))

        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            rows.append(row)
    # note that the order of the results is not guaranteed to be the same as the order of the inputs
    return rows


def get_new_tracking_metadata(rows, old_trm):
    new_trm = old_trm.copy()
    for row in rows:
        state_code = row["StateCode"]
        if state_code not in new_trm:
            new_trm[state_code] = {"last_fetched": row["DateTime"]}
        else:
            old_date = datetime.strptime(
                new_trm[state_code]["last_fetched"], "%Y-%m-%d"
            )
            new_date = datetime.strptime(row["DateTime"], "%Y-%m-%d")
            if new_date > old_date:
                new_trm[state_code]["last_fetched"] = row["DateTime"]
    return new_trm


def update_tracking_metadata(rows):
    tracking_metadata = load_tracking_data()
    tracking_metadata = get_new_tracking_metadata(rows, tracking_metadata)
    save_tracking_data(tracking_metadata)


def run():
    request_inputs = get_request_inputs()

    def process_batch(batch):
        rows = get_data(batch)
        save_data(rows, output_dir)
        update_tracking_metadata(rows)

    batch_size = 500
    batch = []
    for input in request_inputs:
        batch.append(input)
        if len(batch) == batch_size:
            process_batch(batch)
            batch = []
    if batch:
        process_batch(batch)


if __name__ == "__main__":
    run()
