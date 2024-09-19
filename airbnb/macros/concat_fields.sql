{% macro concat_fields(fields = [] , separator = ',') %}

{{ fields | join("||" + "'" + separator + "'" + "||" )   }}

{% endmacro %}

