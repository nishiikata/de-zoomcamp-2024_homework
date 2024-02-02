import pyarrow as pa
import pyarrow.parquet as pq
import os

from mage_ai.data_preparation.shared.secrets import get_secret_value
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

gcs_cred = get_secret_value("gcs_cred_json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcs_cred

bucket_name = get_secret_value("bucket_name")
project_id = get_secret_value("project_id")

table_name = "nyc_taxi_data"

root_path = f"{bucket_name}/{table_name}"

def export_data(data: DataFrame, *args, **kwargs):
    partition_cols=["lpep_pickup_date"]

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=partition_cols,
        filesystem=gcs
    )
