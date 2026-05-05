{{ config(materialized='table') }}

with bel as (
    select * from {{ ref('int_bel_components') }}
),

frame as (
    select distinct cohort_id, sex, scenario_id, issue_age,
        cast(remaining_months / 12 as int) as remaining_term_years
    from {{ ref('int_projection_frame') }}
    where projection_month = 1
),

scenario as (
    select scenario_id, scenario_group
    from {{ ref('stg_stress_scenario_definition') }}
)

select
    b.cohort_id,
    b.sex,
    f.issue_age,
    f.remaining_term_years,
    b.scenario_id,
    s.scenario_group,
    b.version_id,
    b.mort_version_id,
    b.premium_pv,
    b.benefit_pv,
    b.expense_pv,
    b.bel_amount,
    b.policy_count,
    b.bel_per_policy
from bel b
inner join frame f
    on b.cohort_id = f.cohort_id
    and b.sex = f.sex
    and b.scenario_id = f.scenario_id
inner join scenario s
    on b.scenario_id = s.scenario_id