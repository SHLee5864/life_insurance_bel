{{ config(materialized='view') }}

select
    age,
    sex,
    qx_annual,
    source,
    year,
    version_id as mort_version_id
from {{ source('life_insurance_raw', 'mortality_assumption') }}
where version_id in ('MORT_2026_04', 'MORT_EXP_STUDY')