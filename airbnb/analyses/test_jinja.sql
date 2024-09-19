 {{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }} 

'The expression is :' ~ TO_DATE('9999-12-31','YYYY-MM-DD')

 {{ env_var('DBT_PROFILE_DIR','C:\\Users\\conne') }}

 {{   var('prod_db') }}  {# in cmd  set PROD_DB=TESTDB #}

 "test_schema_{{ (run_started_at - modules.datetime.timedelta(0)).strftime('%Y') }}_{{ (run_started_at - modules.datetime.timedelta(0)).strftime('%m')}}_{{ (run_started_at - modules.datetime.timedelta(0)).strftime('%d') }}"

'{{ dbt_utils.pretty_time(format="%Y%m%d") }}'

{{  -1 * var('lookback') }}

{{ dbt_date.today(tz='UTC')   }}

{{ var('dateExpr') }}
{# {{get_column_query(table='dim_hosts_cleaned') }} #}

{{ 'IAS_DEV_WH' if target.name == 'dev' else 'IAS_PROD_WH' }}

{#

{{ dbt_date.last_month()  }}
{{ dbt_date.last_month(tz=None) }} as last_month_start_date

#}

{{this.database}}
{{this.schema}}
{{this.identifier}}


{# {%-  set source_relation = adapter.get_relation(
        database=target.database
        ,schema=target.schema
        ,identifier='dim_hosts_cleaned'
)

%} #}


{# {{  log(" Source Relation: "~ source_relation , info=True ) }}
{{  print(" Source Relation: "~ source_relation ) }} #}


{% for i in range(10) -%} {{i|as_number}} {%- if not loop.last -%},{%- endif -%} {%- endfor %}

{% set schema = 'raw1' %}
{# Make sure to add the expression tag only when it is in a separate line else not required inside a stmt tag #}
{% if schema is not none %}
   {% set modified_schema  = 'dev_' ~ schema  %}    
{% endif %}

{{modified_schema}}

{# If schema exists ,then this method is a no-op #}
{% do adapter.create_schema(api.Relation.create(database=target.database, schema=schema)) %}



SELECT 
IFF((SELECT COUNT(*) FROM airbnb.raw.DIM_HOSTS_CLEANED WHERE HOST_ID IS NULL ) >= 1, 1 , 0) AS check --NOT A VALID SQL
from dual

--QUOTE RESERVED KEYWORDS USING adapter.quote
SELECT 
IFF((SELECT COUNT(*) FROM airbnb.raw.DIM_HOSTS_CLEANED WHERE HOST_ID IS NULL ) >= 1, 1 , 0) AS {{ adapter.quote('check') }}
from dual


{# Exceptions test #}
{% set datestr = 123456 %}
{% if datestr is not string %}

  {# {{ exceptions.raise_compiler_error("The variable is expected to be in a string format , Got " ~ datestr~ " instead.") }} #}
  
    {{ exceptions.warn("The variable is expected to be in a string format , Got " ~ datestr~ " instead.") }}

{% endif %}


{{ teamlist() }}


{% set persons = ['Sachi','Som','Arpit'] %}
{% set people = persons|join(',')|as_text %}
{{log(  "INFO: people var is of type string!"|as_bool if people is string   , info=True)}}


 {% set relation = 'AIRBNB.RAW.MARKETUNIT' %}
 {% set field = 'MarketUnit' %}
{% set marketunit = get_col_values(relation , field)   %}
{{ generate_case_stmt(marketunit) }}

{# 
{{dbt_date.today()}}
{{dbt_date.now()}}
{{dbt_date.day_of_week(dbt_date.today())}} #}
{# {{convert_timezone(column, target_tz=None, source_tz=None)}} #}

