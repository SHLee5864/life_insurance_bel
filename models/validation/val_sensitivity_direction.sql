{{ config(materialized='view') }}

with base as (
    select cohort_id, sex, bel_amount as bel_base
    from {{ ref('int_bel_components') }}
    where scenario_id = 'BASE'
),

stressed as (
    select cohort_id, sex, scenario_id, bel_amount as bel_stressed
    from {{ ref('int_bel_components') }}
    where scenario_id != 'BASE'
),

joined as (
    select
        s.cohort_id,
        s.sex,
        s.scenario_id,
        b.bel_base,
        s.bel_stressed,
        s.bel_stressed - b.bel_base as delta_bel
    from stressed s
    inner join base b
        on s.cohort_id = b.cohort_id
        and s.sex = b.sex
)

select
    *,
    case
        when scenario_id = 'MORT_UP' and delta_bel < 0 then 'FAIL'
        when scenario_id = 'MORT_DOWN' and delta_bel > 0 then 'FAIL'
        when scenario_id = 'RATE_UP' and delta_bel > 0 then 'FAIL'
        when scenario_id = 'RATE_DOWN' and delta_bel < 0 then 'FAIL'
        else 'PASS'
    end as sensitivity_ok
from joined