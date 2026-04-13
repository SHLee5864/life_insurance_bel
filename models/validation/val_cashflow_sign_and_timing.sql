{{ config(materialized='view') }}

with checks as (
    select
        cohort_id,
        sex,
        scenario_id,
        sum(case when cashflow_type = 'premium' and cashflow_amount > 0 then 1 else 0 end) as premium_sign_fail,
        sum(case when cashflow_type = 'death_benefit' and cashflow_amount < 0 then 1 else 0 end) as benefit_sign_fail,
        sum(case when cashflow_type = 'expense' and cashflow_amount < 0 then 1 else 0 end) as expense_sign_fail,
        sum(case when cashflow_type = 'premium' and cashflow_timing != 'month_start' then 1 else 0 end) as premium_timing_fail,
        sum(case when cashflow_type in ('death_benefit', 'expense') and cashflow_timing != 'month_end' then 1 else 0 end) as benefit_expense_timing_fail
    from {{ ref('int_cashflow_lines') }}
    group by cohort_id, sex, scenario_id
)

select
    *,
    case
        when premium_sign_fail + benefit_sign_fail + expense_sign_fail
            + premium_timing_fail + benefit_expense_timing_fail > 0 then 'FAIL'
        else 'PASS'
    end as cashflow_ok
from checks