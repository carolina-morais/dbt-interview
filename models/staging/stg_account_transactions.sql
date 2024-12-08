--interval could be extended to account for late arriving data
{% set partitions_to_replace = ['current_date','date_sub(current_date, interval 1 day)'] %}

{{
  config(
    schema = 'staging',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'transaction_date', 'data_type': 'date'},
    partitions = partitions_to_replace,
    full_refresh = false

  )
}}

select 
    {{ dbt_utils.generate_surrogate_key(['account_id_hashed', 'date']) }}  as surrogate_key
  , date                                                                   as transaction_date
  , account_id_hashed
  , transactions_num                                                       as no_of_transactions
  , {{ add_metadata() }}                                                 

from {{ source('monzo_warehouse', 'account_transactions') }}

{% if is_incremental() %}
where date in ({{ partitions_to_replace | join(',') }})
{% endif %}

qualify row_number() over(partition by surrogate_key) = 1