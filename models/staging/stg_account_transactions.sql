{{
  config(
    materialized = 'table',
    )
}}

--TODO: get rid of *
select *
from {{ source('monzo_warehouse', 'account_transactions') }}