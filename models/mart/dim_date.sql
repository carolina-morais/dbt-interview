{{
  config(
    schema = 'mart',
    materialized = 'table'
    )
}}

with date_spine as (
    select 
        date                                    as date_day
    from unnest(generate_date_array('2017-08-09', current_date)) as date
)


select
    date_day
    , date_trunc(date_day, week)                as date_week
    , date_trunc(date_day, week(monday))        as date_week_iso
    , {{add_metadata()}}  
    
from date_spine