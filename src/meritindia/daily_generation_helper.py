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

merit_domain = "meritindia.in"
merit_ip = "45.249.235.16"
url = f"https://{merit_ip}/StateWiseDetails/GetStateWiseDetailsForPiChart"
plant_url = f"https://{merit_ip}/StateWiseDetails/GetPowerStationData"
timezone = "Asia/Kolkata"
state_starting_date = datetime(2017, 6, 1, tzinfo=pytz.timezone(timezone)).date()
plant_starting_date = datetime(2021, 6, 1, tzinfo=pytz.timezone(timezone)).date()
tracking_base_path = Path("../../data/meritindia/track")
state_codes_path = Path("./state_codes.json")
output_dir = Path("../../data/meritindia/")
max_workers = 10
batch_size = 100  # how often to save the data to disk


def get_track_path(data_type):
    return tracking_base_path / f"{data_type}.json"


def load_tracking_data(data_type):
    tracking_path = get_track_path(data_type)
    with open(tracking_path, "r") as file:
        tracking_data = json.load(file)
    return tracking_data


def load_state_codes() -> dict[str, str]:
    with open(state_codes_path, "r") as file:
        state_codes = json.load(file)
    return state_codes


def save_tracking_data(data_type: str, tracking_data: dict):
    tracking_path = get_track_path(data_type)
    with open(tracking_path, "w") as file:
        json.dump(tracking_data, file, indent=4)


def get_merit_format_date(date: str):
    _date = datetime.strptime(date, "%Y-%m-%d").date()
    return _date.strftime("%d %b %Y")


def ist_now():
    return datetime.now(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M:%S")


def get_rows_by_state(rows: list[dict]):
    rows_by_state = {}
    for row in rows:
        state_code = row["StateCode"]
        if state_code not in rows_by_state:
            rows_by_state[state_code] = []
        rows_by_state[state_code].append(row)
    return rows_by_state


def save_data(data_type: str, rows: list[dict]):
    if data_type == "daily-state-generation":
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
    elif data_type == "daily-plant-generation":
        headers = [
            "StateCode",
            "DateTime",
            "PowerStationName",
            "NonSchedule",
            "Schedule",
            "ChartShowingScheduleValue",
            "ChartShowingNonScheduleValue",
            "TypeOfGeneration",
            "fetched_at",
        ]
    dest_dir = output_dir / data_type / "raw"
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
    req_date_str = get_merit_format_date(date)
    res = requests.request(
        "POST",
        url,
        data={"StateCode": state_code, "date": req_date_str},
        verify=False,
        headers={"Host": merit_domain},
    )
    data = res.json()
    row = {
        "fetched_at": ist_now(),
        "StateCode": state_code,
        "DateTime": date,
    }
    for item in data:
        row[item["TypeOfEnergy"]] = item["EnergyValue"]

    return [row], res.elapsed.total_seconds()


def get_daily_plant_generation(state_code, date: str):
    req_date_str = get_merit_format_date(date)
    res = requests.request(
        "POST",
        plant_url,
        data={"StateCode": state_code, "date": req_date_str},
        verify=False,
        headers={"Host": merit_domain},
    )
    data = res.json()
    row = {
        "fetched_at": ist_now(),
        "StateCode": state_code,
        "DateTime": date,
    }
    plant_rows = []
    for item in data:
        plant_row = row.copy()
        plant_row.update(item)
        plant_rows.append(plant_row)

    return plant_rows, res.elapsed.total_seconds()


def get_request_inputs(data_type: str) -> Iterable[tuple[str, str]]:
    starting_date = (
        state_starting_date
        if data_type == "daily-state-generation"
        else plant_starting_date
    )
    tracking_metadata = load_tracking_data(data_type)
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


def get_data(data_type, request_inputs: list[tuple[str, str]]):
    if data_type == "daily-state-generation":
        data_getter = get_daily_state_generation
    elif data_type == "daily-plant-generation":
        data_getter = get_daily_plant_generation
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    all_rows = []
    total_latency = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for input in request_inputs:
            futures.append(executor.submit(data_getter, *input))

        for future in concurrent.futures.as_completed(futures):
            rows, latency = future.result()
            total_latency += latency
            all_rows.extend(rows)
    # note that the order of the results is not guaranteed to be the same as the order of the inputs
    print("Average latency:", total_latency / len(request_inputs))
    return all_rows


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


def update_tracking_metadata(data_type: str, rows: list[dict]):
    tracking_metadata = load_tracking_data(data_type)
    tracking_metadata = get_new_tracking_metadata(rows, tracking_metadata)
    save_tracking_data(data_type, tracking_metadata)


def run():
    data_type = "daily-state-generation"

    def process_batch(batch):
        rows = get_data(data_type, batch)
        save_data(data_type, rows)
        update_tracking_metadata(data_type, rows)

    request_inputs = get_request_inputs(data_type)
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
