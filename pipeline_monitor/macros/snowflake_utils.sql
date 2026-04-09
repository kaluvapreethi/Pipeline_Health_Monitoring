{% macro custom_set_query_tag() %}
  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {# Include the resource type (model, seed, snapshot) in the JSON #}
  {% set tag_json = '{
    "app": "dbt", 
    "node_name": "' ~ model.name ~ '", 
    "node_resource_type": "' ~ model.resource_type ~ '",
    "invocation_id": "' ~ invocation_id ~ '"
  }' %}
  
  {% set query_tag_command = "alter session set query_tag = '" ~ tag_json ~ "';" %}
  {{ return(query_tag_command) }}
{% endmacro %}