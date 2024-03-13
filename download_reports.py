import pytz
from datetime import datetime, timedelta
from pathlib import Path
import requests
import json
import zipfile
from parse_reports import (
    get_trnsformed_df,
    write_to_csv,
)

temp_output_dir = Path("./data/npp/daily-generation/raw/")
processed_output_dir = Path("./data/npp/daily-generation/csv/")
zip_dir = Path("./data/npp/daily-generation/raw/")
track_json_path = Path("./data/npp/daily-generation/track.json")
timezone = pytz.timezone("Asia/Kolkata")
start_date = datetime(2017, 9, 1).replace(tzinfo=timezone)
track_json = None
existing_reports = None


def bootstrap():
    temp_output_dir.mkdir(parents=True, exist_ok=True)
    global track_json
    global existing_reports
    if not track_json_path.exists():
        track_json = {"failed": {}}
        write_json(track_json, track_json_path)
    else:
        with open(track_json_path, "r") as file:
            track_json = json.load(file)

    existing_reports = get_all_reports()


def download_file(url, output_path: Path):
    response = requests.get(url)
    response.raise_for_status()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as file:
        file.write(response.content)


def get_file_format(date):
    if date < datetime(2018, 3, 31).replace(tzinfo=timezone):
        # based on manual inspection, some reports in march 2018 could be available in xls also
        return "pdf"
    else:
        return "xls"


def get_failed_dates_map():
    return {date["date"]: date for date in track_json.get("failed", [])}


def flush_track_json():
    write_json(track_json, track_json_path)


def update_latest_downloaded_date(date):
    # update if the date is greater than the current latest downloaded date
    date_str = date.strftime("%Y-%m-%d")
    if "lastest_downloaded_date" not in track_json:
        track_json["latest_downloaded_date"] = date_str

    current_date = ist_parse(track_json["latest_downloaded_date"])
    if date > current_date:
        track_json["latest_downloaded_date"] = date_str


def write_json(data, dest_path):
    with open(dest_path, "w") as file:
        json.dump(data, file, indent=4)


def ist_now():
    return datetime.now(timezone)


def ist_parse(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone)


def get_dates_to_download():
    latest_downloaded_date = track_json.get("latest_downloaded_date", None)
    end_date = ist_now() - timedelta(days=2)
    if latest_downloaded_date is None:
        start_with = start_date
    else:
        start_with = ist_parse(latest_downloaded_date) + timedelta(days=1)
    dates_to_retry = []
    for date_str, date_info in track_json["failed"].items():
        date = ist_parse(date_str)
        if (ist_now() - date).days < 30:
            dates_to_retry.append(date)

    dates = []
    for day in range((end_date - start_with).days + 1):
        date = start_with + timedelta(days=day)
        if date not in dates_to_retry:
            dates.append(date)
    return dates_to_retry + dates


def get_all_reports() -> list[str]:
    zip_files = list(zip_dir.glob("*.zip"))
    existing_files = []
    for zip_file in zip_files:
        all_files = get_zip_files(zip_file)
        existing_files.extend(all_files)
    return existing_files


def get_zip_files(zip_file_path):
    all_files = []
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        for file_info in zip_ref.infolist():
            all_files.append(file_info.filename)
    return all_files


def add_files_to_zip(
    zip_file_path: Path, files_to_add: list[Path], directory_in_zip: str
):
    # create the zip file if it doesn't exist
    if not zip_file_path.exists():
        with zipfile.ZipFile(zip_file_path, "w") as zip_file:
            pass
    with zipfile.ZipFile(
        zip_file_path, "a", compression=zipfile.ZIP_DEFLATED
    ) as zip_ref:
        for file_to_add in files_to_add:
            arcname = f"{zip_file_path.stem}/{directory_in_zip}/{file_to_add.name}"
            zip_ref.write(file_to_add, arcname=arcname)


def report_already_downloaded(file_name: str, date, format) -> bool:
    zip_file_name = f"{date.year}/{format}/{file_name}"
    return zip_file_name in existing_reports


bootstrap()


def get_report_url(date: datetime) -> str:
    date_str = date.strftime("%Y-%m-%d")
    base_url_path = date.strftime("%d-%m-%Y")
    format = get_file_format(date)
    file_name = f"dgr2-{date_str}.{format}"
    return (
        f"https://npp.gov.in/public-reports/cea/daily/dgr/{base_url_path}/{file_name}"
    )


def get_temp_output_path(date, format):
    date_str = date.strftime("%Y-%m-%d")
    file_name = f"dgr2-{date_str}.{format}"
    dest_dir = temp_output_dir / format
    return dest_dir / file_name


dates_to_download = get_dates_to_download()
for date in dates_to_download:
    date_str = date.strftime("%Y-%m-%d")
    format = get_file_format(date)
    url = get_report_url(date)
    file_name = Path(url).name
    output_path = get_temp_output_path(date, format)
    if not report_already_downloaded(file_name, date, format):
        try:
            download_file(url, output_path)
            # create a zip file for each year and store the file in it
            zip_file_path = zip_dir / f"{date.year}.zip"
            add_files_to_zip(zip_file_path, [output_path], directory_in_zip=format)
            print(f"Downloaded report for {date_str}")

            if date_str in track_json["failed"]:
                track_json["failed"].pop(date_str)
            update_latest_downloaded_date(date)
            flush_track_json()
        except requests.exceptions.HTTPError as e:
            print(f"Failed to download report for {date_str}: {e}")
            track_json["failed"][date_str] = {
                "url": url,
                "response_code": e.response.status_code,
            }
            update_latest_downloaded_date(date)
            flush_track_json()
            continue
        output_path.unlink()
    else:
        update_latest_downloaded_date(date)
        flush_track_json()
