{{ config(materialized='view') }}

select
    cohort_id,
    sex,
    scenario_id,
    premium_pv,
    benefit_pv,
    expense_pv,
    bel_amount,
    abs(benefit_pv + expense_pv + premium_pv - bel_amount) as bel_error,
    case
        when abs(benefit_pv + expense_pv + premium_pv - bel_amount) > 0.01 then 'FAIL'
        else 'PASS'
    end as bel_reconciled
from {{ ref('int_bel_components') }}