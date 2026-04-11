{{ config(materialized='view') }}

with src as (

    select
        cohort_id,
        sex,
        issue_age,
        term_years,
        maturity_age,
        sum_assured,
        policy_count,
        elapsed_duration_years,
        version_id as cohort_version_id
    from {{ source('life_insurance_raw', 'policy_cohort_input') }}

),

final as (

    select
        cohort_id,
        sex,
        issue_age,
        term_years,
        maturity_age,
        sum_assured,
        policy_count,
        elapsed_duration_years,

        -- 허용 파생
        issue_age + elapsed_duration_years as attained_age,
        term_years - elapsed_duration_years as remaining_term_years,

        cohort_version_id
    from src
)

select * from final
