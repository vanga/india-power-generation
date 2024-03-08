"""
Only handles xls reports.
PDF files are parsed to csv, but the work to parse them and conveert to structred data is not done yet
"""

import zipfile
import itertools
import tabula
import csv
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import concurrent.futures

pd.options.mode.chained_assignment = None


src_dir = Path("./data/raw/")

output_dir = Path("./data/csv/")
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
    formats = ["pdf", "xls"]
    for file in src_dir.iterdir():
        if file.suffix == ".zip":
            # unzip file using python module, suggest code
            with zipfile.ZipFile(file, "r") as zip_ref:
                zip_ref.extractall(src_dir)
    for report in itertools.chain(src_dir.glob("**/*.xls"), src_dir.glob("**/*.pdf")):
        if report.suffix in data_exts:
            yield report


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
    print(f"Number of tables found in {report_path.name}: {len(tables)}")
    df = pd.DataFrame()
    for i, table in enumerate(tables):
        df = pd.concat([df, table], ignore_index=True)
    df.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)


def convert_excel_to_csv(report_path: Path, output_path: Path):
    df = pd.read_excel(report_path)
    df.to_csv(output_path, index=False)


def convert_report_to_csv(report_path: Path):
    output_path = output_dir / report_path.with_suffix(".csv").name
    ext = report_path.suffix
    assert ext in data_exts
    src_report_format = report_path.suffix[1:]
    format_dir = output_dir / src_report_format
    format_dir.mkdir(parents=True, exist_ok=True)
    output_path = format_dir / report_path.with_suffix(".csv").name
    if output_path.exists():
        return
    print("Converting", report_path, "to", output_path)
    if src_report_format == "pdf":
        convert_pdf_to_csv(report_path, output_path)
    elif src_report_format == "xls":
        convert_excel_to_csv(report_path, output_path)


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
    if df.shape[1] != expected_columns:
        print(f"Extra columns found in {date}")
        return
    return df


def add_additional_columns(df):
    """
    Fill up the plant metadata from earlier rows
    Add date and format columns
    """
    o_power_st_col = 2
    o_sector_col = 5
    o_station_type_col = 4
    o_unit_no_col = 3
    region = None
    state = None
    sector = None
    type = None
    station = None

    df.insert(0, row_type_col, pd.NA)
    df.insert(1, region_col, pd.NA)
    df.insert(2, state_col, pd.NA)
    df.insert(3, sector_col, pd.NA)
    df.insert(4, station_type_col, pd.NA)
    df.insert(5, station_col, pd.NA)
    df.insert(6, unit_col, pd.NA)
    faulty_dates = set()

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

    for idx, row in tqdm(df.iterrows()):
        date = row[0]
        if date in faulty_dates:
            continue
        # if it is pd.NA add it to faulty_dates
        if pd.isnull(row[o_power_st_col]):
            faulty_dates.add(date)
            continue
        if is_region_row(row):
            # set region to value from previous row
            region = df.iloc[idx - 1][o_power_st_col]
            df.at[idx, row_type_col] = "Region"
            state, sector, type, station = None, None, None, None
        elif is_state_row(row):
            # set state to value from previous row
            state = df.iloc[idx - 1][o_power_st_col]
            df.at[idx, row_type_col] = "State"
            sector, type, station = None, None, None
        elif is_sector_row(row):
            sector = row[o_sector_col]
            df.at[idx, row_type_col] = "Sector"
            type, station = None, None

        elif is_type_row(row):
            type = row[o_station_type_col]
            df.at[idx, row_type_col] = "Station Type"
            station = None

        elif is_station_row(row):

            station = row[o_power_st_col]
            df.at[idx, row_type_col] = "Station"

        elif is_unit_row(row):
            # unit row
            unit = row[o_power_st_col] + " " + str(int(float(row[o_unit_no_col])))
            df.at[idx, unit_col] = unit
            df.at[idx, row_type_col] = "Unit"
        else:
            continue

        df.at[idx, region_col] = region
        df.at[idx, state_col] = state
        df.at[idx, sector_col] = sector
        df.at[idx, station_type_col] = type
        df.at[idx, station_col] = station

    df.drop(
        columns=[o_power_st_col, o_sector_col, o_station_type_col, o_unit_no_col],
        inplace=True,
    )
    print(f"Faulty dates: {faulty_dates}")
    return df


def get_merged_df(format) -> pd.DataFrame:
    """
    Add additional columns for format and date and return a combined df
    """
    all_rows = []
    failed_dates = []
    for report in tqdm((output_dir / format).iterdir()):
        if report.suffix != ".csv":
            continue
        date = report.stem.split("-", maxsplit=1)[1]
        df = pd.read_csv(report, header=None)
        df = clean_report(df, format, date)
        if df is None:
            failed_dates.append(date)
            continue
        df.insert(0, date_col, date)
        df.insert(1, format_col, format)
        all_rows.extend(df.values)

    merged_df = pd.DataFrame(all_rows)
    return merged_df, failed_dates


def write_to_csv(all_df: pd.DataFrame, output_dir: Path):
    region_df = all_df[all_df[row_type_col] == "Region"]
    state_df = all_df[all_df[row_type_col] == "State"]
    sector_df = all_df[all_df[row_type_col] == "Sector"]
    station_type_df = all_df[all_df[row_type_col] == "Station Type"]
    station_df = all_df[all_df[row_type_col] == "Station"]
    unit_df = all_df[all_df[row_type_col] == "Unit"]

    region_df.drop(
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
    ).to_csv(output_dir / "region.csv", index=False)
    state_df.drop(
        columns=[
            "Row Type",
            "Sector",
            "Station Type",
            "Station",
            "Unit",
            "Source Format",
            "Outage Type",
        ]
    ).to_csv(output_dir / "state.csv", index=False)
    sector_df.drop(
        columns=[
            "Row Type",
            "Station Type",
            "Station",
            "Unit",
            "Source Format",
            "Outage Type",
        ]
    ).to_csv(output_dir / "sector.csv", index=False)
    station_type_df.drop(
        columns=["Row Type", "Station", "Unit", "Source Format", "Outage Type"]
    ).to_csv(output_dir / "station_type.csv", index=False)
    station_df.drop(
        columns=["Row Type", "Unit", "Source Format", "Outage Type"]
    ).to_csv(output_dir / "station.csv", index=False)
    unit_df.drop(columns=["Row Type", "Source Format"]).to_csv(
        output_dir / "unit.csv", index=False
    )
    all_df.to_csv(output_dir / "all.csv", index=False)


output_dir.mkdir(parents=True, exist_ok=True)

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    reports = list(reports_to_parse())
    res = executor.map(convert_report_to_csv, reports)


executor.shutdown(wait=True)

xls_df, failed_dates = get_merged_df("xls")
xls_df.columns = range(len(xls_df.columns))
all_df = add_additional_columns(xls_df)
all_df.columns = get_final_columns()

write_to_csv(all_df, output_dir)
