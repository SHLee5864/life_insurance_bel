{{ config(materialized='view') }}

with mortality as (
    select * from {{ ref('int_projection_mortality_rates') }}
),

lapse as (
    select * from {{ ref('int_projection_lapse_rates') }}
),

frame as (
    select * from {{ ref('int_projection_frame') }}
),

combined as (
    select
        f.cohort_id,
        f.sex,
        f.scenario_id,
        f.projection_month,
        f.policy_count,
        m.scenario_qx_monthly as qx,
        l.scenario_lapse_monthly as lx,

        -- 월별 생존율 (사망도 해지도 안 한 비율)
        (1 - m.scenario_qx_monthly) * (1 - l.scenario_lapse_monthly) as monthly_survival

    from frame f
    inner join mortality m
        on f.cohort_id = m.cohort_id
        and f.sex = m.sex
        and f.scenario_id = m.scenario_id
        and f.projection_month = m.projection_month
    inner join lapse l
        on f.cohort_id = l.cohort_id
        and f.sex = l.sex
        and f.scenario_id = l.scenario_id
        and f.projection_month = l.projection_month
),

rollforward as (
    select
        cohort_id,
        sex,
        scenario_id,
        projection_month,
        policy_count,
        qx,
        lx,

        -- inforce_open: month 1 = policy_count, 이후 = 누적생존 × policy_count
        policy_count * coalesce(
            exp(
                sum(ln(monthly_survival))
                over (
                    partition by cohort_id, sex, scenario_id
                    order by projection_month
                    rows between unbounded preceding and 1 preceding
                )
            ),
            1.0
        ) as inforce_open

    from combined
)

select
    cohort_id,
    sex,
    scenario_id,
    projection_month,
    policy_count,
    inforce_open,
    inforce_open * qx as expected_deaths,
    inforce_open * (1 - qx) as post_death_inforce,
    inforce_open * (1 - qx) * lx as expected_lapses,
    inforce_open * (1 - qx) * (1 - lx) as inforce_close

from rollforward