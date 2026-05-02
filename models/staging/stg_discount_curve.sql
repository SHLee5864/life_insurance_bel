{{ config(materialized='view') }}

select
    tenor_month,
    zero_rate_annual,
    version_id
from {{ source('life_insurance_raw', 'discount_curve') }}
