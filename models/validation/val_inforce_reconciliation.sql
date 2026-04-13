{{ config(materialized='view') }}

with checks as (
    select
        cohort_id,
        sex,
        scenario_id,
        projection_month,
        inforce_open,
        expected_deaths,
        expected_lapses,
        inforce_close,
        policy_count,
        abs(inforce_open - expected_deaths - expected_lapses - inforce_close) as identity_error,
        case when inforce_open < 0 or inforce_close < 0 or expected_deaths < 0 or expected_lapses < 0
            then 1 else 0
        end as negative_flag
    from {{ ref('int_inforce_rollforward') }}
)

select
    cohort_id,
    sex,
    scenario_id,
    max(identity_error) as max_identity_error,
    sum(negative_flag) as negative_count,
    min(case when projection_month = 1 then inforce_open end) as first_month_open,
    min(case when projection_month = 1 then policy_count end) as policy_count,
    case
        when max(identity_error) > 0.001 then 'FAIL'
        when sum(negative_flag) > 0 then 'FAIL'
        when min(case when projection_month = 1 then inforce_open end)
            != min(case when projection_month = 1 then policy_count end) then 'FAIL'
        else 'PASS'
    end as inforce_ok
from checks
group by cohort_id, sex, scenario_id