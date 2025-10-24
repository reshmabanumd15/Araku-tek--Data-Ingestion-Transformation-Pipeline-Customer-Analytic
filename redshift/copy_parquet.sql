-- Replace <ARN_ROLE>, <REGION>, <BUCKET>
COPY ca_pro.stg_customer
FROM 's3://<BUCKET>/processed/customer/'
IAM_ROLE '<ARN_ROLE>'
FORMAT AS PARQUET
REGION '<REGION>';

COPY ca_pro.stg_transactions
FROM 's3://<BUCKET>/processed/transactions/'
IAM_ROLE '<ARN_ROLE>'
FORMAT AS PARQUET
REGION '<REGION>';
