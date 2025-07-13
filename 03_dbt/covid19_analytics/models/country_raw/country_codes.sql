{{ config(materialized='table') }}

SELECT *
FROM {{ ref('country_codes') }}