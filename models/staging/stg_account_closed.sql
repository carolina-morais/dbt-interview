{{
  config(
    materialized = 'table',
    )
}}


select 
closed_ts
, account_id_hashed
from {{ source('monzo_warehouse', 'account_closed') }}
qualify row_number() over(partition by account_id_hashed, closed_ts) = 1