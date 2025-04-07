{% macro grant_select(role) %}

{% do log("Waiting for system privileges to be assigned...", info=True)   %}

{% set grant_sql_stmts %}
grant usage on database {{target.database}} to role {{role}};
grant usage on schema {{target.schema}} to role {{role}};
grant select on all tables in schema {{target.schema}} to role {{role}};
grant select on all views in schema {{target.schema}} to role {{role}};



{# grant usage on schema STAGING to role {{role}};
grant select on all tables in schema DBT_HOL_DEV.STAGING to role {{role}}; -- dbt_prod_role  can be assigned schema access by dev roles if they have been granted "WITH GRANT OPTION" PRIV
grant select on all views in schema DBT_HOL_DEV.STAGING to role {{role}}; --  dbt_prod_role #}

{% endset %}


{% do run_query(grant_sql_stmts) %}
{% do log("Privileges successfully granted to role " ~ role  ~ "..." , info=True)    %}



{% endmacro %}

