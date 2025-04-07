 {% macro get_dyn_tbl_name_1(tbl_prefix) %}

   {% set dt = modules.datetime %}
   {% set rd = modules.dateutil.relativedelta %}  {# import not working for dateutil module #}

   {% set now = dt.datetime.now() %}
   {% set today = dt.date.today() %}
   {% set dt_local = modules.pytz.timezone("US/Eastern").localize(now) %}

   {% set a_month_ago = now - rd.relativedelta(months=1) %}
   {% set month = a_month_ago.strftime("%b") %}
   {% set year = a_month_ago.strftime("%Y") %}

  {{ return(tbl_prefix ~ "_" ~ month ~ year) }}


{% endmacro %}


