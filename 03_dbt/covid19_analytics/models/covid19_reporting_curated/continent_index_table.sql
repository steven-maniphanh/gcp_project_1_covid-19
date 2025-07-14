{{ config(materialized='table') }}

SELECT *
FROM {{ ref('continent_index_table') }}