 {{ config(materialized='table') }}

with core as (
    select * from {{ ref('mart_bel_components_core') }}
),

base as (
    select
        scenario_id,
        version_id,
        mort_version_id,
        sum(bel_amount) as total_bel
    from core
    where scenario_id = 'BASE'
    group by scenario_id, version_id, mort_version_id
),

sensitivity as (
    select
        scenario_id,
        scenario_group,
        version_id,
        mort_version_id,
        sum(delta_bel) as total_delta_bel,
        case
            when sum(abs(bel_base)) = 0 then null
            else sum(delta_bel) / sum(abs(bel_base))
        end as weighted_avg_delta_pct
    from {{ ref('mart_bel_sensitivity') }}
    group by scenario_id, scenario_group, version_id, mort_version_id
)

select
    'BASE' as scenario_id,
    null as scenario_group,
    b.version_id,
    b.mort_version_id,
    b.total_bel,
    cast(0 as double) as total_delta_bel,
    cast(0 as double) as weighted_avg_delta_pct
from base b

union all

select
    s.scenario_id,
    s.scenario_group,
    s.version_id,
    s.mort_version_id,
    b.total_bel + s.total_delta_bel as total_bel,
    s.total_delta_bel,
    s.weighted_avg_delta_pct
from sensitivity s
inner join base b
    on s.version_id = b.version_id
    and s.mort_version_id = b.mort_version_id