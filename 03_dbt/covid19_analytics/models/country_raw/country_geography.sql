{{ config(materialized='table') }}

SELECT *
FROM {{ ref('country_geography') }}
ORDER BY country_name