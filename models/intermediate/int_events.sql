{{
  config(
    materialized = 'view',
    )
}}

select
created_ts                              as event_timestamp
, account_id_hashed
, account_type
, user_id_hashed
, 'Opened'                             as event_type
, {{add_metadata()}}  

from {{ ref('stg_account_created') }}

union all

select 
closed_ts                              as event_timestamp
, account_id_hashed
, null                                 as account_type
, null                                 as user_id_hashed
, 'Closed'                             as event_type
, {{add_metadata()}}  
from {{ ref('stg_account_closed') }}

union all

select 
reopened_ts                            as event_timestamp
, account_id_hashed
, null                                 as account_type
, null                                 as user_id_hashed
, 'Reopened'                           as event_type
, {{add_metadata()}}  
from {{ ref('stg_account_reopened') }}
