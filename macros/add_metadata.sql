{% macro add_metadata() %}

/* this macro should contain all the important metadata fields for a dbt run*/
current_timestamp()             as dbt_run_processed_on_ts

{% endmacro %}
