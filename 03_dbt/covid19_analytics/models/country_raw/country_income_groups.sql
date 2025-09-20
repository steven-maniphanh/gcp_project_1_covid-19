{{ config(materialized='table') }}

SELECT *
FROM {{ ref('country_income_groups') }}
ORDER BY country_code