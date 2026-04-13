{{ config(materialized='view') }}

with frame as (
    select * from {{ ref('int_projection_frame') }}
),

lapse as (
    select * from {{ ref('stg_lapse_assumption') }}
),

final as (
    select
        f.cohort_id,
        f.sex,
        f.scenario_id,
        f.projection_month,
        f.duration_year,

        l.lapse_rate_annual as base_lapse_annual,

        1 - pow(1 - l.lapse_rate_annual, 1.0/12) as base_lapse_monthly,

        f.lapse_multiplier,

        least(1.0,
            greatest(0.0,
                (1 - pow(1 - l.lapse_rate_annual, 1.0/12)) * f.lapse_multiplier
            )
        ) as scenario_lapse_monthly

    from frame f
    left join lapse l
        on f.duration_year = l.duration_year
)

select * from final