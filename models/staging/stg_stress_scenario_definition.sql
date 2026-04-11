{{ config(materialized='view') }}

with src as (

    select
        scenario_id,
        scenario_type,
        coalesce(mortality_multiplier, 1.0) as mortality_multiplier,
        coalesce(lapse_multiplier,      1.0) as lapse_multiplier,
        coalesce(discount_shift_bps,    0  ) as discount_shift_bps,
        scenario_group,
        is_base_scenario,
        scenario_description
    from {{ source('life_insurance_raw', 'stress_scenario_definition') }}

),

final as (

    select
        scenario_id,
        scenario_type,
        mortality_multiplier,
        lapse_multiplier,
        discount_shift_bps,
        scenario_group,
        is_base_scenario,
        scenario_description
    from src
)

select * from final
