{{ config(materialized='view') }}

with frame as (
    select
        cohort_id,
        sex,
        scenario_id,
        min(projection_month) as min_month,
        max(projection_month) as max_month,
        count(*) as row_count,
        max(remaining_months) as remaining_months
    from {{ ref('int_projection_frame') }}
    group by cohort_id, sex, scenario_id
)

select
    cohort_id,
    sex,
    scenario_id,
    min_month,
    max_month,
    row_count,
    remaining_months,
    case
        when min_month != 1 then 'FAIL'
        when max_month != remaining_months then 'FAIL'
        when row_count != remaining_months then 'FAIL'
        else 'PASS'
    end as projection_ok
from frame