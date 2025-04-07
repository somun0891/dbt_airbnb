{% set tbl_relation = adapter.get_relation(
                        database = target.database,
                        schema = target.schema,
                        identifier = 'cars'
                        )
%}              

{{ log(tbl_relation , info=True) }}

{% set relation_exists = tbl_relation is not none %}

{{ log(relation_exists , info=True) }}

{% set tbl_name = tbl_relation.name %} {# relation name #}

{{ log(tbl_name , info=True) }} 

{% set tbl_type = tbl_relation.type %} {# relation type #}

{{ log(tbl_type , info=True) }}

{% set collist = [] %}
{% set colinfo = [] %}
{# 
[SnowflakeColumn(column='ID', dtype='NUMBER', char_size=None, numeric_precision=38, numeric_scale=0),
 SnowflakeColumn(column='MODEL', dtype='VARCHAR', char_size=50, numeric_precision=None, numeric_scale=None), 
 SnowflakeColumn(column='BRAND', dtype='VARCHAR', char_size=40, numeric_precision=None, numeric_scale=None), 
 SnowflakeColumn(column='COLOR', dtype='VARCHAR', char_size=30, numeric_precision=None, numeric_scale=None), 
 SnowflakeColumn(column='MAKE', dtype='NUMBER', char_size=None, numeric_precision=38, numeric_scale=0)]
#}
 {% set columnsObject = adapter.get_columns_in_relation( relation = tbl_relation) %} 

{% for col in columnsObject %}
    {% do collist.append(col.column) %}
    {{ colinfo.append({col.name : col.dtype}) }}
{% endfor %}

{{ print(collist) }}
{{ print(colinfo) }}
{{ print(log("The column object :" ~ columnsObject , info=True)) }}

{% set get_brand   %}
    select brand from cars
{% endset %}
    
{{ log("The brand is :" ~ get_brand , info=True) }}

{% if execute %}
    {% set results = run_query(get_brand) %}
    {% set result_list = results.rows %}
    {% set single_value = results.columns[0][0] %}
{% else %}
    {% set results = [] %}
    {% set single_value = None %}
    {% set result_list = [] %}
{% endif %}

 {{ log(single_value, info=True) }}
 {{ log(result_list, info=True) }}
 
 {% for row in result_list %}
   {% set brand = row[0] %}
    {{ log(brand | string , info=True) }}
 {% endfor %}

{% set today = modules.datetime.datetime.today() %}
{% set now = modules.datetime.datetime.now() %}
{% set yesterday = today - modules.datetime.timedelta(days = 1) %}
{% set local = now.astimezone(modules.pytz.timezone('America/New_York')) %}
{% set difference = modules.datetime.timedelta(days = 0)  - modules.datetime.timedelta(days = 50) %}
{% set now_fmt = now.strftime("%Y-%m-%d") %}
{{ log(today, info=True) }}
{{ log(yesterday, info=True) }}
{{ log(local, info=True) }}
{{ log(difference.days, info=True) }}
{{ log(now_fmt, info=True) }}

{# convert json to dict #}
{% set my_json_str = '{"abc": 123}' %}
{% set my_dict = fromjson(my_json_str) %}
{% do log(my_dict['abc'], true) %}

{{ target.name == 'dev' | as_bool }}

{{ 'dev' if target.name == 'dev' else 'prod' }}

{% set attr = [
                (1 ,'component_type'),
                (2,'account_type') , 
                (3,'account_sub_type')
              ] 
 %}

{% set case_str = '' %}
{% set case_list = [] %}

{% for key,item in attr %}
    
    {% set case_str -%}
         case when {{key}} = s.key then {{item}} end as {{item}}_cd {% if not loop.last %}, {% endif %} 
    {% endset %}

   {{ case_list.append(case_str | replace('\n' , '') ) }}
   
{% endfor %}

{% set case_str = case_list | join("\n") %}  

{{ log(case_str , True) }}
{% set is_env_dev = var('debug' , false) | as_native %}

{{ log(is_env_dev , false) }}
{{ log('env type: ' ~ is_env_dev is boolean , true) }}

{% set name = 'sachi' %}
{% set names_list = ['sachi', 'rima', 'noor'] %}
{% set cnt = 3 %}

{{ name_list is sequence }}
{{ name_list is iterable }}
{{ name is iterable }}
{{ name is string }}
{{ cnt is number }}

{{ adapter.quote('column1') }}

 {% set fields_expr = [] %}
{% set blank = "''" %}
{% set fieldlist = ['cola' , 'colb' , 'colc'] %}

 {% for field in  fieldlist  %}
  {% do fields_expr.append(  
    'coalesce(cast( ' + field + ' as ' + dbt.type_string() + ') , ' + blank + ' ) '
  )
  %}
  {% endfor %}

{{ fields_expr | join(', \n') }}


