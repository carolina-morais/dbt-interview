{{
  config(
    materialized = 'table',
    )
}}


select 
reopened_ts
, account_id_hashed
from {{ source('monzo_warehouse', 'account_reopened') }}
qualify row_number() over(partition by account_id_hashed, reopened_ts) = 1