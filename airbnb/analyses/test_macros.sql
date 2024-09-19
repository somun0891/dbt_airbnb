 {% set fieldlist = ['col1','col2'] %}
{# {{ dbt_utils.generate_surrogate_key(fields) }}  #}

 {% set fields = [] %}
{% set blank = "''" %}
 {% for field in  fieldlist  %}
  {% do fields.append(  
    "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ ") , "  ~ blank ~ " )"
  )
  %}

   {% if not loop.last %}
   {% do fields.append("'-'") %}
   {% endif %}

 {% endfor %}
{# dbt.concat(fields)  #}
 {{log(dbt.hash(dbt.concat(fields))  ,info=true)}}

  {{log(dbt_utils.generate_surrogate_key(['cola' , 'colb'])  ,info=true)}}



 {% set fields = ['col1' , 'col2' , 'col3']  %}
 {% set fields_w_prefix = [] %}

 {# Expected output - select t.col1 , t.col2 , t.col3  from mytable; #}

{% for field in fields %}
  {% set fieldstring = 't.' + field %}
  {% do fields_w_prefix.append(fieldstring) %}
{% endfor %}

 {% set cols = fields_w_prefix | join(', ') %}

 select 
    {{ cols }}
from  
    mytable t;

{% set singlevalue = 'Som' %}
{% set myinteger = 100 %}
{% set nameinlowercase = 'maven' %}
{% set exists = true %}
{% set names = ['SABYA' ,' SACHI' ,'Nayak ','Som']  %}
{% set names_lower = names | map("trim") | map("lower") | list %}
{% set namesstring = 'SABYA,SACHI,NAYAK' %}
{% set nameslist = namesstring.split(',') %}
{{nameslist}}


{% for name in names %}
 {%- if loop.index is odd or loop.index == 0 -%}
  {{name | upper}}
 {%- endif -%}
{% endfor %}

{% set query = "select cast( " + dbt.string_literal(names_lower| join(',')) + " as " +dbt.type_string() + ") " %}
{{query}}

{# {{names|pprint}} #}

{{ names_lower is defined }}
{{ names is iterable }}
{{ singlevalue is string }}
{{ myinteger is integer }}
{{ nameinlowercase is lower }}
{{ exists is boolean }}
{{ names is mapping }}

{{singlevalue not in names }}

{{singlevalue != namesstring }}

    {% set table_clone = api.Relation.create(
            database = this.database,
            schema = this.schema,
            identifier = new_prefix ~ this.identifier ) %}
{{table_clone}}

{# {{this.database}}
{{this.schema}}
{{this.identifier}} #}

{% set prefix =  "CLN_" %}
{% set table_clone = this.database +"."+ this.schema +"."+ prefix + this.identifier  %}
CREATE OR REPLACE {{table_clone}} CLONE {{this}}




{% set model  = '"TICKIT".raw.users' %}
{% set compare_model  = '"TICKIT".raw.users_clone' %}
{% set group_by_cols = ['USERID','USERNAME']  %}
{% set default_value = 1 %}
{% set join_cols %}
    {%- for col in group_by_cols  %} 
     and a.{{col}} = b.{{col}} 
    {%- endfor -%}
{% endset %}

{% if group_by_cols is defined and group_by_cols | length() > 0 %}
  {% set select_cols = group_by_cols | join(',') + ',' %}
  {% set group_by = 'group by ' + group_by_cols | join(',') + ','  %}
{% endif %}

with a as 
(
	SELECT 
		{{select_cols}} 
    {{default_value}} as default_a
		,count(*) as count_a
	FROM 
		{{model}}
	{{group_by}}
    {{default_value}}
), b as 
(
	SELECT 
		{{select_cols}} 
    {{default_value}} as default_b
		,count(*) as count_b
	FROM 
		{{compare_model}} 
	{{group_by}}
   {{default_value}}
)
SELECT 
  {% for col in group_by_cols  %}
  a.{{col}}  AS {{col}}_a,
  b.{{col}}  AS {{col}}_b,
  {%- endfor %}
  count_a,
  count_b, 
  abs(COALESCE(count_a ,0) - COALESCE(count_b,0)) as diff_count
FROM a 
FULL JOIN b on 
  a.default_a = b.default_b
  {{ join_cols }}
HAVING 
  diff_count > 0; 


{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_product_tier', 'dim_product_tier'),
    ('dim_date', 'dim_date')
    ])

}}


 --select * from {{get_snapshot('listing_snapshot')}}

{{ run_started_at.strftime('%Y-%m-%d %H:%M:%S.%f') }}

{# import module short hand #}
{% set dt = modules.datetime %} 

{% set now = dt.datetime.now() %}
{% set three_days_ago_iso = (now - modules.datetime.timedelta(3)).isoformat() %}
{# weekday #}
{{ now }}
{{now.isoweekday()}}
{{now.month}}
{{now.day}}
{{now.hour}}
{{now.minute}}

{# Local date #}
{% set dt = dt.datetime(2024, 10, 27, 6, 0, 0) %}
{% set dt_local = modules.pytz.timezone('US/Eastern').localize(dt) %}

{{dt_local}}

{{now + modules.datetime.timedelta(days=10, seconds=15)}} --2024-03-03 21:06:25.762565

{% set difference =  (dt.strptime('22-Feb-2024','%d-%b-%Y') - dt.strptime('18-Feb-2024','%d-%b-%Y')) %}
{{ difference.days }}

{{ config.get('name')  }} --None


{{ concat_fields(['firstname','lastname']) }}

{% if var('Success' , false  )%}
true
{% else %}
false
{% endif %}
