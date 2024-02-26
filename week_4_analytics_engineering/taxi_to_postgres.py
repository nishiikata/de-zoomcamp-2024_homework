import time

import pandas as pd

from functools import wraps
from itertools import groupby, product
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Iterable, TypedDict

from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema


if TYPE_CHECKING:
    from pandas import DataFrame
    from sqlalchemy import Engine


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Took {total_time:.4f} seconds to run {func.__name__}")
        return result

    return timeit_wrapper


@timeit
def main():
    """
    Sample script that exports the taxi data used for week 4
    to a PostgreSQL Database Server
    """
    config = dotenv_values("./dev.env")
    url = config.get("DATABASE_URL")
    src_path = config.get("SOURCE_PATH", "")
    file_extension = config.get("SOURCE_FORMAT", "csv.gz")
    schema = config.get("DB_SCHEMA", None)

    yellow = fetch_taxi_paths(
        src_path, "yellow", years=[2019, 2020], file_extension=file_extension
    )
    green = fetch_taxi_paths(
        src_path, "green", years=[2019, 2020], file_extension=file_extension
    )
    fhv = fetch_taxi_paths(src_path, "fhv", years=[2019], file_extension=file_extension)
    taxi_paths = yellow + green + fhv

    engine = create_engine(url)
    print("Starting export of taxi data to sql...")
    export_taxi_csv_to_sql(taxi_paths, con=engine, schema=schema)


def fetch_taxi_paths(
    folder_path: str = "",
    label: Literal["yellow", "green", "fhv"] = "fhv",
    *,
    years: Iterable[int] = (2019,),
    months: Iterable[int] = range(1, 12 + 1),
    file_extension: Literal["csv", "csv.gz"] = "csv.gz",
) -> list[tuple[str, Literal["yellow", "green", "fhv"]]]:
    if folder_path == "":
        folder_uri = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{label}/"
        )
    else:
        folder_uri = folder_path

    taxi_data_paths: list[tuple[str, Literal["yellow", "green", "fhv"]]] = []
    year_month = product(years, months)
    for year, month in year_month:
        file_uri = f"{folder_uri}{label}_tripdata_{year}-{month:02d}.{file_extension}"
        taxi_data_paths.append((file_uri, label))

    return taxi_data_paths


def export_taxi_csv_to_sql(
    taxi_data_paths: list[tuple[str, Literal["yellow", "green", "fhv"]]],
    *,
    con: "Engine",
    chunksize: int = 131_072,
    schema: str | None = None,
    BLOCK_SIZE: int | None = 131_072,
    replace_existing_tables: bool = True,
):
    if replace_existing_tables:
        for key, group in groupby(taxi_data_paths, lambda x: x[1]):
            first_path: str = next(group)[0]
            filepath = first_path if Path(first_path).is_file() else ""
            label: Literal["yellow", "green", "fhv"] = key

            _init_empty_taxi_table(
                filepath=filepath,
                label=label,
                con=con,
                schema=schema,
            )

    for filepath, label in taxi_data_paths:
        if (
            filepath.endswith(".csv")
            and con.url.drivername == "postgresql+psycopg"
            and Path(filepath).is_file()
        ):
            _ingest_with_copy_from(
                filepath,
                label=label,
                con=con,
                BLOCK_SIZE=BLOCK_SIZE,
            )
        else:
            _ingest_with_insert_multi(
                filepath,
                label=label,
                chunksize=chunksize,
                con=con,
                schema=schema,
            )


@timeit
def _init_empty_taxi_table(
    filepath: str = "",
    *,
    label: Literal["yellow", "green", "fhv"],
    con: "Engine",
    schema: str | None = None,
):
    if filepath == "":
        match label:
            case "yellow":
                filepath = (
                    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
                    "yellow/yellow_tripdata_2020-04.csv.gz"
                )
            case "green":
                filepath = (
                    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
                    "green/green_tripdata_2020-04.csv.gz"
                )
            case "fhv":
                filepath = (
                    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
                    "fhv/fhv_tripdata_2019-09.csv.gz"
                )
    label_spec: _TaxiTableSchema = _get_taxi_table_schema(label)
    if schema is not None:
        with con.connect() as connection:
            connection.execute(CreateSchema(schema, if_not_exists=True))
            connection.commit()

    df = pd.read_csv(
        filepath,
        dtype=label_spec["dtypes"],
        nrows=100,
        parse_dates=label_spec["parse_dates"],
        compression="gzip" if filepath.endswith(".csv.gz") else None,
        date_format="%Y-%m-%d %H:%M:%S",
    )
    df = df.head(0)
    df.to_sql(
        name=label_spec["table_name"],
        con=con,
        schema=schema,
        if_exists="replace",
        index=False,
    )


@timeit
def _ingest_with_copy_from(
    filepath: str,
    label: Literal["yellow", "green", "fhv"],
    con: "Engine",
    schema: str | None = None,
    *,
    BLOCK_SIZE: int | None = None,
):
    label_spec: _TaxiTableSchema = _get_taxi_table_schema(label)

    filename: str = filepath.split(sep="/")[-1]
    schema_prefix = f"{schema}." if schema is not None else ""
    table_name: str = label_spec["table_name"]
    current_time: str = time.strftime("%x %X %z")
    print(f"Appending {filename} data to {table_name} at {current_time}")

    with (
        Session(bind=con) as session,
        session.begin(),
        open(filepath, "r") as f,
    ):
        cursor = session.connection().connection.cursor()
        with cursor.copy(
            f"COPY {schema_prefix}{table_name} FROM STDIN delimiter ',' CSV HEADER"
        ) as copy:
            while data := f.read(BLOCK_SIZE):
                copy.write(data)


@timeit
def _ingest_with_insert_multi(
    filepath: str,
    /,
    *,
    schema: str | None = None,
    label: Literal["yellow", "green", "fhv"],
    chunksize: int,
    con: "Engine",
):
    label_spec: _TaxiTableSchema = _get_taxi_table_schema(label)

    dtypes: dict = label_spec["dtypes"]
    parse_dates: list[str] = label_spec["parse_dates"]
    table_name: str = label_spec["table_name"]

    with pd.read_csv(
        filepath,
        dtype=dtypes,
        parse_dates=parse_dates,
        chunksize=chunksize,
        compression="gzip" if filepath.endswith(".csv.gz") else None,
        date_format="%Y-%m-%d %H:%M:%S",
    ) as reader:
        filename = filepath.split(sep="/")[-1]
        schema_prefix = f"{schema}." if schema is not None else ""
        current_time: str = time.strftime("%b %m %X %:z")
        print(
            f"Appending {filename} data to {schema_prefix}{table_name} at {current_time}"
        )

        for chunk in reader:
            df: DataFrame = chunk
            df.to_sql(
                name=table_name,
                con=con,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=65_536 // len(df.columns),
                method="multi",
            )


class _TaxiTableSchema(TypedDict):
    dtypes: dict
    parse_dates: list[str]
    table_name: str


def _get_taxi_table_schema(
    label: Literal["yellow", "green", "fhv"]
) -> _TaxiTableSchema:
    match label:
        case "yellow":
            dtypes = {
                "VendorID": pd.CategoricalDtype([1, 2]),
                "passenger_count": pd.UInt8Dtype(),
                "trip_distance": float,
                "PULocationID": pd.UInt16Dtype(),
                "DOLocationID": pd.UInt16Dtype(),
                "RatecodeID": pd.CategoricalDtype([1, 2, 3, 4, 5, 6]),
                "store_and_fwd_flag": pd.CategoricalDtype(["Y", "N"]),
                "payment_type": pd.CategoricalDtype([1, 2, 3, 4, 5, 6]),
                "fare_amount": float,
                "extra": float,
                "mta_tax": float,
                "improvement_surcharge": float,
                "tip_amount": float,
                "tolls_amount": float,
                "total_amount": float,
                "congestion_surcharge": float,
            }
            parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
            table_name = "yellow_tripdata"
        case "green":
            dtypes = {
                "VendorID": pd.CategoricalDtype([1, 2]),
                "passenger_count": pd.UInt8Dtype(),
                "trip_distance": float,
                "PULocationID": pd.UInt16Dtype(),
                "DOLocationID": pd.UInt16Dtype(),
                "RatecodeID": pd.CategoricalDtype([1, 2, 3, 4, 5, 6]),
                "store_and_fwd_flag": pd.CategoricalDtype(["Y", "N"]),
                "payment_type": pd.CategoricalDtype([1, 2, 3, 4, 5, 6]),
                "fare_amount": float,
                "extra": float,
                "mta_tax": float,
                "improvement_surcharge": float,
                "tip_amount": float,
                "tolls_amount": float,
                "total_amount": float,
                "trip_type": pd.CategoricalDtype([1, 2]),
                "congestion_surcharge": float,
                "ehail_fee": float,
            }
            parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
            table_name = "green_tripdata"
        case "fhv":
            dtypes = {
                "dispatching_base_num": str,
                "PUlocationID": pd.UInt16Dtype(),
                "DOlocationID": pd.UInt16Dtype(),
                "SR_Flag": float,
                "Affiliated_base_number": str,
            }
            parse_dates = ["pickup_datetime", "dropOff_datetime"]
            table_name = "fhv_tripdata"

    return {
        "dtypes": dtypes,
        "parse_dates": parse_dates,
        "table_name": table_name,
    }


if __name__ == "__main__":
    main()
