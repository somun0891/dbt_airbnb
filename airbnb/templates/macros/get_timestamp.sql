{%  macro  get_timestamp(current_ts)  -%}

{% set results = [] %}

{%set filter = 'DBT_INTERNAL_DEST.AUDIT_TS >  TO_TIMESTAMP_NTZ(' ~ current_ts ~')'%}


{# TO_TIMESTAMP_NTZ({{current_ts}}  ) #}
{% do results.append(filter) %}

{{return(results)}}

{%-  endmacro %}