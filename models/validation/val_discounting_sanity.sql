{{ config(materialized='view') }}

with checks as (
    select
        cohort_id,
        sex,
        scenario_id,
        version_id,
        sum(case when discount_factor <= 0 then 1 else 0 end) as df_non_positive,
        sum(case when discount_factor is null then 1 else 0 end) as df_null
    from {{ ref('int_cashflows_discounted') }}
    group by cohort_id, sex, scenario_id, version_id
)

select
    *,
    case
        when df_non_positive > 0 then 'FAIL'
        when df_null > 0 then 'FAIL'
        else 'PASS'
    end as discount_ok
from checks