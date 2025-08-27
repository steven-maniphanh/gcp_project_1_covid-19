{{ config(materialized='table') }}

SELECT 
    c1.country_name,
    c1.alpha_3_code AS country_code,
    c2.region,
    c2.continent,
    c2.area_km2,
    c3.income_group
FROM {{ref('country_codes')}} AS c1
LEFT JOIN {{ref('country_geography')}} as c2
ON c1.alpha_3_code = c2.country_code
LEFT JOIN {{ref('country_income_groups')}} as c3
ON c1.alpha_3_code = c3.country_code