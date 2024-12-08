{{
  config(
    materialized = 'table',
    schema = 'metrics'
    )
}}

with activity_rate as(
    select
        date_day 
    ,   sum(is_active_in_last_7_days)        as sum_is_active_in_last_7_days
    ,   nullif(sum(is_account_open), 0)       as sum_is_account_open
    ,   sum(is_active_in_last_7_days) * 1.0 / nullif(sum(is_account_open), 0)   as active_rate_7d

    from {{ ref('user_activity_by_day') }}
    group by date_day
)

select
    date_day
,   sum_is_active_in_last_7_days
,   sum_is_account_open
,   active_rate_7d

from activity_rate
