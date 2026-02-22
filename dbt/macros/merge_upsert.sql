
{% macro merge_upsert(target_table, source_query, unique_key, update_columns) %}

merge into {{ target_table }} as target
using ({{ source_query }}) as source
on target.{{ unique_key }} = source.{{ unique_key }}

when matched then
  update set
    {% for col in update_columns %}
    target.{{ col }} = source.{{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}

when not matched then
  insert (
    {% for col in update_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
  )
  values (
    {% for col in update_columns %}
    source.{{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
  )

{% endmacro %}
