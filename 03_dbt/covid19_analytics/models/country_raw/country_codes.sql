{{ config(materialized='table') }}

SELECT *
FROM {{ ref('country_codes') }}
ORDER BY country_name