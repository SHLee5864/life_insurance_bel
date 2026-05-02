{{ config(materialized='view') }}

with discounted as (
    select * from {{ ref('int_cashflows_discounted') }}
),

aggregated as (
    select
        cohort_id,
        sex,
        scenario_id,
        version_id,

        sum(case when cashflow_type = 'premium' then discounted_cashflow else 0 end) as premium_pv,
        sum(case when cashflow_type = 'death_benefit' then discounted_cashflow else 0 end) as benefit_pv,
        sum(case when cashflow_type = 'expense' then discounted_cashflow else 0 end) as expense_pv

    from discounted
    group by cohort_id, sex, scenario_id, version_id
),

final as (
    select
        a.cohort_id,
        a.sex,
        a.scenario_id,
        a.version_id,
        a.premium_pv,
        a.benefit_pv,
        a.expense_pv,
        a.benefit_pv + a.expense_pv + a.premium_pv as bel_amount,
        f.policy_count,
        (a.benefit_pv + a.expense_pv + a.premium_pv) / f.policy_count as bel_per_policy

    from aggregated a
    inner join (
        select distinct cohort_id, sex, scenario_id, policy_count
        from {{ ref('int_inforce_rollforward') }}
        where projection_month = 1
    ) f
        on a.cohort_id = f.cohort_id
        and a.sex = f.sex
        and a.scenario_id = f.scenario_id
)

select * from final