select *
    from 
        {{ source('feature_store_source','fraud_transactions') }}
