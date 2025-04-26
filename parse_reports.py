"""
Only handles xls reports.
PDF files are parsed to csv, but the work to parse them and conveert to structred data is not done yet
TODO:
* Away to not process all the reports for a fresh clone.
* Process PDF reports
"""

import traceback
import zipfile
import itertools
import tabula
import csv
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import concurrent.futures

pd.options.mode.chained_assignment = None


src_dir = Path("./data/npp/daily-generation/raw/")

output_dir = Path("./data/npp/daily-generation/csv/")
data_exts = [".pdf", ".xls"]
no_of_workers = 10
row_type_col = "Row Type"
region_col = "Region"
state_col = "State"
sector_col = "Sector"
station_type_col = "Station Type"
station_col = "Station"
unit_col = "Unit"
date_col = "Date"
format_col = "Source Format"
"""
The regions in this dataset are based on power grid regions and hence these are the only regions
1) Northern
2) Western
3) Southern
4) Eastern
5) North Eastern
"""


def get_final_columns():
    return [
        row_type_col,
        region_col,
        state_col,
        sector_col,
        station_type_col,
        station_col,
        unit_col,
        date_col,
        format_col,
        "Outage Type",
        "Monitored CAP in MW",
        "Generation / Today's Program",
        "Generation / Today's Actual",
        "Generation / FY YTD Program",
        "Generation / FY YTD Actual",
        "Coal Stock in Days",
        "CAP under outage",
        "Outage Date",
        "Expected Date / Sync Date",
        "Remarks",
    ]


def date_from_report_name(report_path):
    return report_path.stem.split("-", maxsplit=1)[1]


def clean_df(df):
    for column in df.columns:
        if df[column].dtype == "object" or df[column].dtype == "string":
            df[column] = (
                df[column]
                .astype("string")
                .str.replace("\n|\r|\t", " ")
                .str.replace("\r", " ")
                .str.replace("\t", " ")
            )
            # replace multiple spaces with single space
            df[column] = df[column].str.replace("\\s+", " ", regex=True)
            df[column] = df[column].str.strip()

        # drop column if all values are null/nan or empty strings
    df.replace("", pd.NA, inplace=True)
    df.dropna(how="all", axis=1, inplace=True)
    df.dropna(how="all", axis=0, inplace=True)


def drop_unnecessary_rows(df):
    # drop rows if all values are null/nan or empty strings or if the row contains any of the following strings
    filter_values = [
        "REGION WISE",
        "POWER STATION",
        "OPERATION PERFORMANCE MONITORING DIVISION",
    ]
    for column in df.columns:
        if df[column].dtype == "object" or df[column].dtype == "string":
            df = df[
                ~df[column].str.contains("|".join(filter_values), regex=True, na=False)
            ]
    df.reset_index(drop=True, inplace=True)
    return df


def reports_to_parse():
    # reports that are not processed yet based on which years data is already part of region.csv
    region_src = output_dir / "region.csv"
    if region_src.exists():
        df = pd.read_csv(region_src)
        dates = df[date_col].unique()
    else:
        dates = []

    for file in src_dir.iterdir():
        if file.suffix == ".zip":
            # unzip file using python module, suggest code
            with zipfile.ZipFile(file, "r") as zip_ref:
                files_in_zip = zip_ref.namelist()
                files_to_parse = []
                for file_in_zip in files_in_zip:
                    file_path = Path(file_in_zip)
                    date = date_from_report_name(file_path)
                    if file_path.suffix == ".xls" and date not in dates:
                        # only process xls reports which are not already processed/ PDF parsing requires special handling
                        files_to_parse.append(file_path)
                if len(files_to_parse) > 0:
                    zip_ref.extractall(
                        src_dir, members=map(lambda x: str(x), files_to_parse)
                    )
                    for report in files_to_parse:
                        yield src_dir / report


def convert_pdf_to_csv(report_path: Path, output_path: Path):
    # tables = camelot.read_pdf(str(report), pages="1-end")
    """
    for ex first pdf report 2017-09-01.pdf itself gets parsed incorrectly
    tabula seems to be better at these tables than camelot. though, camelot also is messing up Unit level generation rows.
    Tabula seems to be also faster than camelot
    """
    tables = tabula.read_pdf(
        str(report_path), pages="all", pandas_options={"header": None}
    )
    df = pd.DataFrame()
    for i, table in enumerate(tables):
        df = pd.concat([df, table], ignore_index=True)
    df.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    return output_path


def convert_excel_to_csv(report_path: Path, output_path: Path):
    df = pd.read_excel(report_path)
    df.to_csv(output_path, index=False)
    return output_path


def convert_report_to_csv(report_path: Path):
    ext = report_path.suffix
    assert ext in data_exts
    src_report_format = report_path.suffix[1:]
    format_dir = output_dir / src_report_format
    format_dir.mkdir(parents=True, exist_ok=True)
    output_path = format_dir / report_path.with_suffix(".csv").name
    if output_path.exists() and output_path.stat().st_size > 0:
        return output_path
    try:
        if src_report_format == "pdf":
            return convert_pdf_to_csv(report_path, output_path)
        elif src_report_format == "xls":
            return convert_excel_to_csv(report_path, output_path)
    except Exception as e:
        print(f"Failed to convert {report_path} to csv")
        print(e)


# clean
def clean_report(df, format, date):
    """
    * replace "nan"
    * drop rows, columns with all nulls
    * drop header, filler rows
    """
    expected_columns = 15 if format == "xls" else 12
    df.replace("nan", pd.NA, inplace=True)
    clean_df(df)
    region_search = df.isin(["NORTHERN"]).any(axis=1)
    if not region_search.any():
        print(f"Region row not found for {date}")
        return
    region_total_row = df[region_search].index[0]
    df = df.iloc[region_total_row:]
    df = drop_unnecessary_rows(df)
    clean_df(df)
    if format == "pdf":
        df = df.astype("object")
        df.insert(1, "Outage Type", pd.NA)
        unit_rows = df[df[0].str.startswith("Unit", na=False)]
        # insert space after "Unit" if not present
        unit_rows[0] = (
            unit_rows[0]
            .str.replace("Unit", "Unit ", regex=False)
            .str.replace("  ", " ", regex=False)
        )
        # set value from 2nd column to Outage Type and shift other column values to left
        unit_rows.loc[:, "Outage Type"] = unit_rows[1]
        unit_rows.iloc[:, 2:] = unit_rows.shift(-1, axis=1).iloc[:, 2:]
        # set all column type to object which will help when updating the main df with shifted unit_rows columns

        df.update(unit_rows)
        # drop last column
        df.drop(df.columns[-1], axis=1, inplace=True)
    if (df.shape[1] != expected_columns) and df.shape[1] != (expected_columns - 1):
        # in some files in later years type and sector are in the same column
        print(f"Extra columns found in {date}", df.columns, expected_columns)
        return
    return df


def add_additional_columns(daily_df):
    """
    Fill up the plant metadata from earlier rows
    Add date and format columns
    """
    o_power_st_col = 2
    o_sector_col = 5 if daily_df.shape[1] == 17 else 4
    o_station_type_col = 4
    o_unit_no_col = 3
    region = None
    state = None
    sector = None
    type = None
    station = None

    daily_df.insert(0, row_type_col, pd.NA)
    daily_df.insert(1, region_col, pd.NA)
    daily_df.insert(2, state_col, pd.NA)
    daily_df.insert(3, sector_col, pd.NA)
    daily_df.insert(4, station_type_col, pd.NA)
    daily_df.insert(5, station_col, pd.NA)
    daily_df.insert(6, unit_col, pd.NA)

    def is_region_row(row):
        return row[o_power_st_col] == "REGION TOTAL"

    def is_state_row(row):
        return row[o_power_st_col] == "STATE TOTAL"

    def is_sector_row(row):
        return row[o_power_st_col].startswith("SECTOR:")

    def is_type_row(row):
        return row[o_power_st_col].startswith("TYPE:")

    def is_station_row(row):
        return (pd.notnull(type) and not pd.notnull(station)) or (
            pd.notnull(station) and not row[o_power_st_col].startswith("Unit")
        )

    def is_unit_row(row):
        return pd.notnull(station) and row[o_power_st_col].startswith("Unit")

    for idx, row in daily_df.iterrows():
        date = row[0]
        # if it is pd.NA add it to faulty_dates
        if pd.isnull(row[o_power_st_col]):
            return None
        if is_region_row(row):
            # set region to value from previous row
            region = daily_df.iloc[idx - 1][o_power_st_col]
            daily_df.at[idx, row_type_col] = "Region"
            state, sector, type, station = None, None, None, None
        elif is_state_row(row):
            # set state to value from previous row
            state = daily_df.iloc[idx - 1][o_power_st_col]
            daily_df.at[idx, row_type_col] = "State"
            sector, type, station = None, None, None
        elif is_sector_row(row):
            sector = row[o_sector_col]
            daily_df.at[idx, row_type_col] = "Sector"
            type, station = None, None

        elif is_type_row(row):
            type = row[o_station_type_col]
            daily_df.at[idx, row_type_col] = "Station Type"
            station = None

        elif is_station_row(row):

            station = row[o_power_st_col]
            daily_df.at[idx, row_type_col] = "Station"

        elif is_unit_row(row):
            # unit row
            unit = row[o_power_st_col] + " " + str(int(float(row[o_unit_no_col])))
            daily_df.at[idx, unit_col] = unit
            daily_df.at[idx, row_type_col] = "Unit"
        else:
            continue

        daily_df.at[idx, region_col] = region
        daily_df.at[idx, state_col] = state
        daily_df.at[idx, sector_col] = sector
        daily_df.at[idx, station_type_col] = type
        daily_df.at[idx, station_col] = station

    daily_df.drop(
        columns=[o_power_st_col, o_sector_col, o_station_type_col, o_unit_no_col],
        inplace=True,
    )

    daily_df.columns = get_final_columns()
    return daily_df


def get_clean_csv(csv_path, format, date) -> pd.DataFrame:
    """
    Add additional columns for format and date and return a combined df
    """

    df = pd.read_csv(csv_path, header=None)
    df = clean_report(df, format, date)
    if df is None:
        return
    df.insert(0, date_col, date)
    df.insert(1, format_col, format)
    return df


def get_trnsformed_df(raw_report) -> pd.DataFrame:
    format = raw_report.suffix[1:]
    date = date_from_report_name(raw_report)
    temp_csv_path = convert_report_to_csv(raw_report)
    if temp_csv_path is None:
        return None
    df = get_clean_csv(temp_csv_path, format, date)
    if df is None:
        return None
    df.columns = range(len(df.columns))
    return add_additional_columns(df)


def add_rows_to_file(file_path, df):
    if not file_path.exists():
        df.to_csv(file_path, index=False)
    else:
        df.to_csv(file_path, mode="a", header=False, index=False)


def write_to_csv(all_df: pd.DataFrame, output_dir: Path):
    region_df = all_df[all_df[row_type_col] == "Region"].drop(
        columns=[
            "Row Type",
            "State",
            "Sector",
            "Station Type",
            "Station",
            "Unit",
            "Source Format",
            "Outage Type",
        ]
    )
    state_df = all_df[all_df[row_type_col] == "State"].drop(
        columns=[
            "Row Type",
            "Sector",
            "Station Type",
            "Station",
            "Unit",
            "Source Format",
            "Outage Type",
        ]
    )
    sector_df = all_df[all_df[row_type_col] == "Sector"].drop(
        columns=[
            "Row Type",
            "Station Type",
            "Station",
            "Unit",
            "Source Format",
            "Outage Type",
        ]
    )
    station_type_df = all_df[all_df[row_type_col] == "Station Type"].drop(
        columns=["Row Type", "Station", "Unit", "Source Format", "Outage Type"]
    )
    station_df = all_df[all_df[row_type_col] == "Station"].drop(
        columns=["Row Type", "Unit", "Source Format", "Outage Type"]
    )
    unit_df = all_df[all_df[row_type_col] == "Unit"].drop(
        columns=["Row Type", "Source Format"]
    )

    add_rows_to_file(output_dir / "region.csv", region_df)
    add_rows_to_file(output_dir / "state.csv", state_df)
    add_rows_to_file(output_dir / "sector.csv", sector_df)
    add_rows_to_file(output_dir / "station_type.csv", station_type_df)
    add_rows_to_file(output_dir / "station.csv", station_df)
    add_rows_to_file(output_dir / "unit.csv", unit_df)


def run():
    # bulk process, useful when doing it for the first time. Else, download_reports already
    output_dir.mkdir(parents=True, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        raw_reports = sorted(list(reports_to_parse()))
        print(f"Found {len(raw_reports)} to convert")
        results = list(tqdm(executor.map(get_trnsformed_df, raw_reports)))

    executor.shutdown(wait=True)
    failed_dates = []
    dfs = []
    for idx, response in enumerate(results):
        if response is None:
            failed_dates.append(raw_reports[idx].stem)
        else:
            dfs.append(response)

    if len(failed_dates) > 0:
        print(f"Failed to parse the following reports: {failed_dates}")

    if len(dfs) > 0:
        all_df = pd.concat(dfs, ignore_index=True)
        write_to_csv(all_df, output_dir)


if __name__ == "__main__":
    run()
