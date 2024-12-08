{% set partitions_to_replace = generate_partitions(var("start_date", generate_start_date(2)), var("end_date", generate_end_date(0)), type='date') %}
{% set incremental_window_start = generate_minimum_partition_value(partitions_to_replace) %}
{% set incremental_window_end = generate_minimum_partition_value(partitions_to_replace, window='end') %}
{{
  config(
    schema = 'mart',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'date_day', 'data_type': 'date'},
    partitions = partitions_to_replace,

  )
}}

with dates as (
    select
        date_day

    from {{ ref('dim_date') }}
    where 1=1

{% if is_incremental() %}
    and date_day between '{{ incremental_window_start }}' and '{{ incremental_window_end }}'
{% endif %}

)

, dim_account as (
    select
        account_id_hashed
    ,   user_id_hashed
    ,   status
    ,   valid_from
    ,   valid_to

    from {{ ref('dim_account') }}
    where 1=1
)

, dim_account_user_mapping as (
    select distinct
        account_id_hashed
    ,   user_id_hashed

    from dim_account
)

, distinct_users as (
    select distinct
        user_id_hashed

    from dim_account
)

, transactions as (
    select
        transaction_date
    ,   account_id_hashed
    ,   no_of_transactions

    from {{ ref('stg_account_transactions') }}
    where 1=1
    {% if is_incremental() %}
    and transaction_date between '{{ incremental_window_start }}' and '{{ incremental_window_end }}'
    {% endif %}

)

, agg_transactions_to_user_date as (
    select
        t.transaction_date
    ,   map.user_id_hashed
    ,   sum(no_of_transactions)                                                 as no_of_transactions

    from transactions t
    left join dim_account_user_mapping map
        on t.account_id_hashed = map.account_id_hashed
    group by all
)

, date_spine_users as (
    select
        d.date_day
    ,   u.user_id_hashed
    from dates d
    cross join distinct_users u
)


, joins as (
    select
        dsu.date_day
    ,   dsu.user_id_hashed
    ,   if(
        exists(
            select 1 from dim_account as da 
            where 1=1
            and timestamp(dsu.date_day) between da.valid_from and da.valid_to
            and dsu.user_id_hashed = da.user_id_hashed
            and da.status in ('Opened', 'Reopened')
            )
        , 1, 0)                                                                 as is_account_open

    ,   coalesce(agg_trx.no_of_transactions, 0)                                 as no_of_transactions
    from date_spine_users dsu
    left join agg_transactions_to_user_date agg_trx
        on dsu.date_day = agg_trx.transaction_date
        and dsu.user_id_hashed = agg_trx.user_id_hashed

) 

{% if is_incremental() %}
, this as (
    select 
        *
    from {{ this }}
    where 1=1
    and date_day = '{{ incremental_window_start }}' - 1 -- look back to previous day outside of incremental window
)

, windows as (
    select
        j.date_day
    ,   j.user_id_hashed
    ,   j.is_account_open
    ,   j.no_of_transactions
    ,   coalesce(last_value(if(j.no_of_transactions > 0, j.date_day, null) ignore nulls) over(partition by j.user_id_hashed order by j.date_day rows between 7 preceding and current row ), if(date_diff(j.date_day, t.date_of_last_activity_in_last_7_days, day) <= 7, t.date_of_last_activity_in_last_7_days, null)) as date_of_last_activity_in_last_7_days
    
    from joins as j
    left join this as t
        on j.user_id_hashed = t.user_id_hashed
    
)
{% else %}

, windows as (
    select
        date_day
    ,   user_id_hashed
    ,   is_account_open
    ,   no_of_transactions
    ,   last_value(if(no_of_transactions > 0, date_day, null) ignore nulls) over(partition by user_id_hashed order by date_day rows between 7 preceding and current row ) as date_of_last_activity_in_last_7_days
    
    from joins
)

{% endif %}

select
        {{ dbt_utils.generate_surrogate_key(['user_id_hashed', 'date_day']) }}  as surrogate_key
    ,   date_day
    ,   user_id_hashed
    ,   is_account_open
    ,   no_of_transactions
    ,   date_of_last_activity_in_last_7_days
    ,   if(date_of_last_activity_in_last_7_days is not null, 1, 0)              as is_active_in_last_7_days
    ,   {{add_metadata()}}  

from windows
where 1=1
{% if is_incremental() %}
and date_day between '{{ incremental_window_start }}' and '{{ incremental_window_end }}'
{% endif %}
