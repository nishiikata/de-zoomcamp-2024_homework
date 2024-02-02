import re

from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df: DataFrame, *args, **kwargs) -> DataFrame:
    mask = (df["passenger_count"] == 0) | (df["trip_distance"] == 0)
    df = df[~mask]
    
    # Question 3 Answer: use ".dt.date" syntax
    df.loc[:, "lpep_pickup_date"] = df["lpep_pickup_datetime"].dt.date

    camel_cols = []
    for s in df.columns:
        if s != s.lower() and s != s.upper() and "_" not in s:
            camel_cols.append(s)

    pattern = re.compile("(?<=[a-z])(?=[A-Z])")
    mapper = {}
    for s in camel_cols:
        mapper[s] = re.sub(pattern, "_", s).lower()

    df = df.rename(columns=mapper)

    # ['VendorID', 'RatecodeID', 'PULocationID', 'DOLocationID']
    # Question 5 Answer: 4 columns
    print(f"length: {len(camel_cols)} columns\n{camel_cols}")

    return df


@test
def test_rows(df: DataFrame) -> None:
    # 139,370 rows
    print("{:,} rows".format(df.shape[0]))

    q2_answers = [
       544_897,
       266_855,
       139_370,
       266_856 
    ]

    assert df.shape[0] in q2_answers, \
        "row count is not in the list of possible answers"


@test
def test_vendor_id(df: DataFrame) -> None:
    # <IntegerArray> [2, 1] Length: 2, dtype: Int64
    # Question 4 Answer: 1 or 2
    print(df["vendor_id"].unique())

    assert "vendor_id" in df, \
        "vendor_id column does not exist"


@test
def test_passenger_count(df: DataFrame) -> None:
    assert not df["passenger_count"].gt(0).any() in df, \
        "passenger_count <= 0"


@test
def test_trip_distance(df: DataFrame) -> None:
    assert not df["trip_distance"].gt(0).any() in df, \
        "trip_distance <= 0"
