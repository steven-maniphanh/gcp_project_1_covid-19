{{ config(materialized='table') }}

SELECT *
FROM {{ ref('world_population') }}
ORDER BY country_name