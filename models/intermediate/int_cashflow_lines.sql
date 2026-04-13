{{ config(materialized='view') }}

with inforce as (
    select * from {{ ref('int_inforce_rollforward') }}
),

premium as (
    select * from {{ ref('stg_premium_input') }}
),

expense as (
    select expense_rate
    from {{ ref('stg_expense_assumption') }}
    where expense_type = 'maintenance'
),

monthly_premium as (
    select
        i.cohort_id,
        i.sex,
        i.scenario_id,
        i.projection_month,
        i.inforce_open,
        i.expected_deaths,
        p.annual_gross_premium / 12.0 as monthly_premium,
        i.policy_count
    from inforce i
    inner join premium p
        on i.cohort_id = p.cohort_id
        and i.sex = p.sex
),

cashflows as (
    -- Premium: negative (insurer inflow), month_start
    select
        cohort_id, sex, scenario_id, projection_month,
        'premium' as cashflow_type,
        -(inforce_open * monthly_premium) as cashflow_amount,
        'month_start' as cashflow_timing
    from monthly_premium

    union all

    -- Death Benefit: positive (insurer outflow), month_end
    select
        mp.cohort_id, mp.sex, mp.scenario_id, mp.projection_month,
        'death_benefit' as cashflow_type,
        mp.expected_deaths * i.sum_assured as cashflow_amount,
        'month_end' as cashflow_timing
    from monthly_premium mp
    inner join {{ ref('int_projection_frame') }} i
        on mp.cohort_id = i.cohort_id
        and mp.sex = i.sex
        and mp.scenario_id = i.scenario_id
        and mp.projection_month = i.projection_month

    union all

    -- Expense: positive (insurer outflow), month_end
    select
        cohort_id, sex, scenario_id, projection_month,
        'expense' as cashflow_type,
        inforce_open * monthly_premium * e.expense_rate as cashflow_amount,
        'month_end' as cashflow_timing
    from monthly_premium, expense e
)

select * from cashflows