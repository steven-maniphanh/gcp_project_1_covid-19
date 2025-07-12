{{ config(materialized='table') }}

SELECT  
  c1.country_name,
  c1.alpha_3_code AS country_code,
  c2.region,
  c2.continent,
  c2.area_km2,
  c3.income_group
FROM `covid19-dbt-analytics-1.covid19_raw_dev.country_codes` AS c1
LEFT JOIN `covid19-dbt-analytics-1.covid19_raw_dev.country_geography` AS c2
  ON c1.alpha_3_code = c2.country_code
LEFT JOIN `covid19-dbt-analytics-1.covid19_raw_dev.country_income_groups` AS c3
  ON c1.alpha_3_code = c3.country_code
ORDER BY country_name