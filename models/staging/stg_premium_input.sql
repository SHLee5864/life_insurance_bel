{{ config(materialized='view') }}

with base_version as (
    select version_id
    from {{ source('life_insurance_raw', 'assumption_version') }}
    where assumption_type = 'pricing'
      and is_base_version = true
),

 src as (

    select
        cohort_id,
        sex,
        annual_gross_premium,
        pricing_assumption_version,
        coalesce(premium_frequency, 'ANNUAL') as premium_frequency,
        coalesce(premium_currency, 'EUR')     as premium_currency
    from {{ source('life_insurance_raw', 'premium_input') }}
    where pricing_assumption_version = (select version_id from base_version)

),

final as (

    select
        cohort_id,
        sex,
        annual_gross_premium,
        pricing_assumption_version,
        premium_frequency,
        premium_currency
    from src
)

select * from final
