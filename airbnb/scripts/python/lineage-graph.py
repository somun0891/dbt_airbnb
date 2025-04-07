from lineagex.lineagex import lineagex

sql_path = 'C:\\Users\\conne\\OneDrive\\Documents\\GitHub\\dbt_airbnb\\airbnb\\target\\compiled\\airbnb\\models\\example'
lineagex(sql=sql_path, target_schema="vantage", search_path_schema="vantage, staging")

#snowflake://<username>:<password>@<account_identifier>.snowflakecomputing.com/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>