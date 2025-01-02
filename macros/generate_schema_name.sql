{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {# staging models go in a global `staging` schema #}
    {% if 'staging' in node.path %}
        {{ 'staging' }}

    {# raw models go in a global `raw` schema #}
    {% elif 'raw' in node.path %}
        {{ 'raw' }}    

    {# marts models go in a global `marts` schema #}
    {% elif 'marts' in node.path %}
        {{ 'marts' }}

    {# intermediate models go in a global `intermediate` schema #}
    {% elif 'intermediate' in node.path %}
        {{ 'intermediate' }}

    {# non-specified schemas go to the default target schema #}
    {% elif custom_schema_name is none %}
        {{ default_schema }}

    {# specified custom schema names go to the schema name prepended with the default schema name in prod #}
    {% elif target.name == 'prod' %}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {# specified custom schemas go to the default target schema for non-prod targets #}
    {% else %}
        {{ default_schema }}
    {% endif %}

{% endmacro %}
