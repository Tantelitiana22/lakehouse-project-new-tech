{% macro ensure_schema(schema_name) %}
    {% set statement %}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
    {% endset %}
    {% do run_query(statement) %}
    {% do log('Ensured lakehouse schema ' ~ schema_name, info=True) %}
{% endmacro %}
