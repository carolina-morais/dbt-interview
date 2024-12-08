{# refresh previous 2 days to account for late arriving data. #}
{% set partitions_to_replace = generate_partitions(var("start_date", generate_start_date(2)), var("end_date", generate_end_date(0)), type='timestamp') %}

{{
  config(
    schema = 'staging',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'created_ts', 'data_type': 'timestamp'},
    partitions = partitions_to_replace,
    full_refresh = false

  )
}}

select 
  {{ dbt_utils.generate_surrogate_key(['account_id_hashed', 'created_ts']) }} as surrogate_key
  , created_ts
  , account_type
  , account_id_hashed
  , user_id_hashed
  , {{add_metadata()}}                                                 

from {{ source('monzo_warehouse', 'account_created') }}

{% if is_incremental() %}
where created_ts in ({{ partitions_to_replace | join(',') }})
{% endif %}

qualify row_number() over(partition by surrogate_key) = 1