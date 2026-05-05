{{ config(materialized='table') }}

with core as (
    select * from {{ ref('mart_bel_components_core') }}
),

base as (
    select cohort_id, sex, version_id, mort_version_id, bel_amount as bel_base
    from core
    where scenario_id = 'BASE'
),

stressed as (
    select cohort_id, sex, scenario_id, version_id, mort_version_id, scenario_group, bel_amount as bel_stressed
    from core
    where scenario_id != 'BASE'
)

select
    s.cohort_id,
    s.sex,
    s.scenario_id,
    s.scenario_group,
    s.version_id,
    s.mort_version_id,
    b.bel_base,
    s.bel_stressed,
    s.bel_stressed - b.bel_base as delta_bel,
    case
        when b.bel_base = 0 then null
        else (s.bel_stressed - b.bel_base) / abs(b.bel_base)
    end as delta_bel_pct
from stressed s
inner join base b
    on s.cohort_id = b.cohort_id
    and s.sex = b.sex
    and s.version_id = b.version_id
    and s.mort_version_id = b.mort_version_id