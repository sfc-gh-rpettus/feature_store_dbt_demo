import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

from snowflake.ml.feature_store import (
    FeatureStore,
    FeatureView,
    CreationMode
)

FS_DEMO_DB = 'FEATURE_STORE_DBT_DEMO'
FS_DEMO_SCHEMA = 'DBT_FS_RPETTUS_FS_DBT_DEMO'



def model(dbt, session):
    dbt.config(
        materialized = "table",
        python_version="3.11",
        packages = ["cryptography","snowflake-ml-python==1.6.0"]
    )

    fs = FeatureStore(
    session=session, 
    database=FS_DEMO_DB, 
    name=FS_DEMO_SCHEMA, 
    default_warehouse='WH_DBT',
    creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    spine_df = session.create_dataframe(
    [
        (1, 2443, "2019-09-01 00:00:00.000"), 
        (2, 1889, "2019-09-01 00:00:00.000"),
        (3, 1309, "2019-09-01 00:00:00.000")
    ], 
    schema=["INSTANCE_ID", "CUSTOMER_ID", "EVENT_TIMESTAMP"])

    customers_fv = fs.get_feature_view('customers_features',version='1')

    inference_dataset = fs.retrieve_feature_values(
    spine_df=spine_df,
    features=[customers_fv],
    spine_timestamp_col="EVENT_TIMESTAMP",
    )

    return inference_dataset