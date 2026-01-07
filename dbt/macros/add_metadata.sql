# not being used but an example

{% macro add_metadata() %}
    '{{ var("batch_date") }}'::date as batch_date,
    current_timestamp() as ingestion_ts
{% endmacro %}
