{% macro audit_log(EventName , Start_TS , End_TS , Description ,Errors) -%}

INSERT INTO dbt_hol_dev.public.audit_trail(AuditKey ,EventName,START_TS, END_TS,Description,Errors,Audit_usr_ID)
SELECT
        '{{ invocation_id  }}'
        ,'{{ EventName }}'
        ,'{{ Start_TS }}'
        , NULLIF('{{ END_TS }}' ,'') AS END_TS
        , '{{ model.config.materialized ~ " " ~ model.name ~ " " ~ Description }}'
        ,'{{ Errors}}'
        ,'{{target.user}}'
;

COMMIT;

{%- endmacro   %}