{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'surrogate_key'
    )
}}

{%if is_incremental() %}
  
{%set max_load_timestamp = dbt_utils.get_query_results_as_dict("select coalesce(max(valid_from), '2019-10-01') as col from " ~ this )["col"][0]%}

{% endif %}

with all_events as (
    select
        event_timestamp
        , account_id_hashed
        , account_type
        , user_id_hashed
        , event_type

from {{ ref('int_events') }} 

{%if is_incremental() %}

where event_timestamp > '{{max_load_timestamp}}'

union all

select
valid_from                          as event_timestamp
, account_id_hashed
, account_type
, user_id_hashed
, status                            as event_type

from {{ this }}

{% endif %}

)

, scd2 as(
select
account_id_hashed
, first_value(user_id_hashed ignore nulls) over (partition by account_id_hashed order by event_timestamp rows between unbounded preceding and unbounded following) as user_id_hashed
, first_value(account_type ignore nulls) over (partition by account_id_hashed order by event_timestamp rows between unbounded preceding and unbounded following)  as account_type

, case 
    when event_type in ('Opened', 'Reopened') then 'Open'
    when event_type = 'Closed' then 'Closed'
  end                                                                                      as status

, event_timestamp                                                                          as valid_from
, lead(event_timestamp) over (partition by account_id_hashed order by event_timestamp)         as valid_to
from all_events
)


select
{{ dbt_utils.generate_surrogate_key(['account_id_hashed', 'valid_from']) }}  as surrogate_key
, account_id_hashed -- natural key
, user_id_hashed
, account_type
, status
, valid_from
, coalesce(valid_to, '9999-01-01 00:00:00') as valid_to
, if(valid_to is null, true, false) as is_current
, if(valid_to is null, 'Indicates the record represents the latest version of the natural key', 'Indicates the record is not the latest version') as is_current_description

-- meta
, current_timestamp() as _processed_timestamp
from scd2