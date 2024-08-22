select * from {{ source("feature_store_source","customer_transactions_fraud") }}

