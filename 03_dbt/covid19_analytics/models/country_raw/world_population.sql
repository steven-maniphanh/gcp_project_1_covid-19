{{ config(materialized='table') }}

SELECT *
FROM {{ ref('world_population') }}