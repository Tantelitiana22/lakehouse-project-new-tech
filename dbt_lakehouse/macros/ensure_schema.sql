{% macro ensure_schema(schema_name) %}
    {% set statement %}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
    {% endset %}
    {% do run_query(statement) %}
    {% if execute and graph is defined %}
        {% for source_node in graph.sources.values() %}
            {% if source_node.schema == schema_name %}
                {% set register_table %}
                CREATE TABLE IF NOT EXISTS {{ source_node.schema }}.{{ source_node.name }}
                USING DELTA
                LOCATION 's3://lakehouse/warehouse/{{ source_node.schema }}.db/{{ source_node.name }}'
                {% endset %}
                {% do run_query(register_table) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {% do log('Ensured lakehouse schema ' ~ schema_name, info=True) %}
{% endmacro %}


{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
