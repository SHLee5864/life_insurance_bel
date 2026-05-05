{{ config(materialized='view') }}

with frame as (
    select * from {{ ref('int_projection_frame') }}
),

mortality as (
    select * from {{ ref('stg_mortality_assumption') }}
),

final as (
    select
        f.cohort_id,
        f.sex,
        f.scenario_id,
        f.projection_month,
        m.mort_version_id,

        m.qx_annual as base_qx_annual,
        1 - pow(1 - m.qx_annual, 1.0/12) as base_qx_monthly,
        f.mortality_multiplier,

        least(1.0,
            greatest(0.0,
                (1 - pow(1 - m.qx_annual, 1.0/12)) * f.mortality_multiplier
            )
        ) as scenario_qx_monthly

    from frame f
    inner join mortality m
        on f.attained_age = m.age
        and f.sex = m.sex
)

select * from final