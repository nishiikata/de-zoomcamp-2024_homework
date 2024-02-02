import numpy as np
import pandas as pd

from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs) -> DataFrame:
    url_format = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"
    months = [10, 11, 12]
    dfs: list[DataFrame] = []

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'improvement_surcharge':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'total_amount':float,
                    'trip_type': pd.Int64Dtype()
                }

    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    for month in months:
        url = url_format.format(month=month)

        df = pd.read_csv(
            url,
            sep=',',
            compression='gzip',
            dtype=taxi_dtypes,
            parse_dates=parse_dates,
            )
        
        dfs.append(df)

    return pd.concat(dfs)


@test
def test_shape(df: DataFrame) -> None:
    # 266,855 rows x 20 columns
    print("{:,} rows x {:,} columns".format(*df.shape))

    q1_answers = [
       (266_855, 20),
       (544_898, 18),
       (544_898, 20),
       (133_744, 20) 
    ]

    assert df.shape in q1_answers, \
        "df.shape is not in the list of possible answers"
