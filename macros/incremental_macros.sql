{%- macro quote_jinja_field(field) -%}
  '{{ field }}'
{%- endmacro -%}

{% macro generate_start_date(delta=None, format='%Y-%m-%d') %}
    {% set delta = delta if delta is not none else 1 %}

    {# Calculate start_date based on the (possibly defaulted) delta #}
    {% set start_date = (modules.datetime.datetime.utcnow() - modules.datetime.timedelta(delta)).strftime(format) %}

    {{ return(start_date | string) }}
{% endmacro %}


{% macro generate_end_date(delta, format = '%Y-%m-%d') %}
  {% set end_date = (modules.datetime.datetime.utcnow() + modules.datetime.timedelta(delta)).strftime(format) %}

  {{ return(end_date | string) }}
{% endmacro %}


{% macro generate_partitions(start_date, end_date, type='date', out_fmt = "%Y-%m-%d") %}

  {% if not end_date %}
    {% set end_date = generate_end_date(0) %}
  {% endif %}

  {% set dates = dates_in_range(start_date, end_date, in_fmt="%Y-%m-%d", out_fmt=out_fmt) %}
    {% set return_obj = [] %}
    {% for x in dates %}
      
      {% if type == 'date' %}
        {% do return_obj.append(quote_jinja_field(x)) %}
      {% elif type == 'timestamp' %}
        {% do return_obj.append('timestamp(' + quote_jinja_field(x) + ')') %}
      {% elif type == 'datetime' %}
        {% do return_obj.append('datetime(' + quote_jinja_field(x) + ')') %}
      {% endif %}
  {% endfor %}

  {{ return(return_obj) }}
{% endmacro %}


{% macro generate_minimum_partition_value(partitions, window='start') %}
    {% if window == 'start' %}
    {% set sql_statement %}
        ( select min(value) from unnest([ {{partitions | join(',')}} ]) value )
    {% endset %}
    {% else %}
    {% set sql_statement %}
        ( select max(value) from unnest([ {{partitions | join(',')}} ]) value )
    {% endset %}
    {% endif %}

  {%- set incremental_date = dbt_utils.get_single_value(sql_statement, default="'2020-01-01'") -%}

  {{ return(incremental_date) }}
{% endmacro %}
