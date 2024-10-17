{{
 config(
 materialized = 'table'
 )
}}

with CTE as (
    select * from DATA360_POC.ODS.ODS_ACCOUNT limit 100
)

select id, del_flg from CTE where del_flg = 'N'