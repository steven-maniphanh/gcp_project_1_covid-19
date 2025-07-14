{{ config(materialized='table') }}

SELECT *
FROM {{ ref('country_income_groups') }}