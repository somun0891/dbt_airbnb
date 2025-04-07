 {% macro get_dyn_tbl_name(tbl_prefix) %}

   {% set dt = modules.datetime %}
   {% set today = dt.date.today() %}

   {% set day_number = dt.datetime.now().day %} 
   {% set now = dt.datetime.now() %}
   {% set dt_local = modules.pytz.timezone("US/Eastern").localize(now) %}    

   {% set last_day_prev_month = now - dt.timedelta(days=day_number) %}
   {% set month = last_day_prev_month.strftime("%b") %}
   {% set year = last_day_prev_month.strftime("%Y") %}

  {{ return(tbl_prefix ~ "_" ~ month ~ year) }}


{% endmacro %}

