{{ config(materialized='view') }}

with cohort as (
    select * from {{ ref('stg_policy_cohort_input') }}
),

scenario as (
    select * from {{ ref('stg_stress_scenario_definition') }}
),

frame_base as (
    select
        c.cohort_id,
        c.sex,
        c.issue_age,
        c.term_years,
        c.elapsed_duration_years,
        c.remaining_term_years,
        c.sum_assured,
        c.policy_count,
        s.scenario_id,
        s.mortality_multiplier,
        s.lapse_multiplier,
        s.discount_shift_bps
    from cohort c
    cross join scenario s
),

projection as (
    select
        fb.cohort_id,
        fb.sex,
        fb.scenario_id,
        fb.issue_age,
        fb.sum_assured,
        fb.policy_count,
        fb.mortality_multiplier,
        fb.lapse_multiplier,
        fb.discount_shift_bps,
        fb.remaining_term_years * 12 as remaining_months,

        pm.projection_month,

        (fb.elapsed_duration_years * 12) + pm.projection_month
            as policy_month,

        cast(
            ceil(((fb.elapsed_duration_years * 12) + pm.projection_month) / 12.0)
            as int
        ) as duration_year,

        fb.issue_age + fb.elapsed_duration_years
            + cast(floor((pm.projection_month - 1) / 12) as int)
            as attained_age,

        cast('{{ var("valuation_date") }}' as date)
            as valuation_date,

        add_months(
            cast('{{ var("valuation_date") }}' as date),
            pm.projection_month - 1
        ) as calendar_month_start,

        add_months(
            cast('{{ var("valuation_date") }}' as date),
            pm.projection_month
        ) as calendar_month_end

    from frame_base fb
    lateral view explode(
        sequence(1, cast(fb.remaining_term_years * 12 as int))
    ) pm as projection_month
)

select * from projection