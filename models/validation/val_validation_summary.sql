{{ config(materialized='view') }}

with proj as (
    select cohort_id, sex, scenario_id, projection_ok from {{ ref('val_projection_continuity') }}
),
inf as (
    select cohort_id, sex, scenario_id, inforce_ok from {{ ref('val_inforce_reconciliation') }}
),
cf as (
    select cohort_id, sex, scenario_id, cashflow_ok from {{ ref('val_cashflow_sign_and_timing') }}
),
disc as (
    select cohort_id, sex, scenario_id, version_id, discount_ok from {{ ref('val_discounting_sanity') }}
),
bel as (
    select cohort_id, sex, scenario_id, version_id, bel_reconciled from {{ ref('val_bel_reconciliation') }}
),
sens as (
    select cohort_id, sex, scenario_id, version_id, sensitivity_ok from {{ ref('val_sensitivity_direction') }}
)

select
    p.cohort_id,
    p.sex,
    p.scenario_id,
    d.version_id,
    p.projection_ok,
    i.inforce_ok,
    c.cashflow_ok,
    d.discount_ok,
    b.bel_reconciled,
    coalesce(s.sensitivity_ok, 'N/A') as sensitivity_ok,
    case
        when p.projection_ok = 'FAIL' then 'FAIL'
        when i.inforce_ok = 'FAIL' then 'FAIL'
        when c.cashflow_ok = 'FAIL' then 'FAIL'
        when d.discount_ok = 'FAIL' then 'FAIL'
        when b.bel_reconciled = 'FAIL' then 'FAIL'
        when coalesce(s.sensitivity_ok, 'PASS') = 'FAIL' then 'FAIL'
        else 'PASS'
    end as overall_validation_status
from proj p
left join inf i on p.cohort_id = i.cohort_id and p.sex = i.sex and p.scenario_id = i.scenario_id
left join cf c on p.cohort_id = c.cohort_id and p.sex = c.sex and p.scenario_id = c.scenario_id
left join disc d on p.cohort_id = d.cohort_id and p.sex = d.sex and p.scenario_id = d.scenario_id
left join bel b on p.cohort_id = b.cohort_id and p.sex = b.sex and p.scenario_id = b.scenario_id and d.version_id = b.version_id
left join sens s on p.cohort_id = s.cohort_id and p.sex = s.sex and p.scenario_id = s.scenario_id and d.version_id = s.version_id