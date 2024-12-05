{{
  config(
    materialized = 'table',
    )
}}

--TODO: Check grain of this event. Cascade to qualify
--TODO:incremental - insert+overwrite
--TODO: process time (CONSISTENCY HERE! (MACRO!)).
-- TODO: surrogate key
-- TODO: documentation - KISS do not go ham with chatgpt. Do these are source and cascade downstream.
select 
created_ts
, account_type
, account_id_hashed
, user_id_hashed

from {{ source('monzo_warehouse', 'account_created') }}
qualify row_number() over(partition by account_id_hashed, created_ts) = 1