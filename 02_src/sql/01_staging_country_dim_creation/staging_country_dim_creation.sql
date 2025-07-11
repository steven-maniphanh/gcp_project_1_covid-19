CREATE OR REPLACE TABLE `covid19-1-463112.staging_tranverse_dataset.country_dim` AS(
SELECT  
  c1.country_name,
  c1.alpha_3_code AS country_code,
  c2.region,
  c2.continent,
  c2.area_km2,
  c3.income_group
FROM `covid19-1-463112.raw_transverse_dataset.country_codes` AS c1
LEFT JOIN `covid19-1-463112.raw_transverse_dataset.country_geography` AS c2
ON c1.alpha_3_code = c2.country_code
LEFT JOIN `covid19-1-463112.raw_transverse_dataset.raw_country_income_groups` AS c3
ON c1.alpha_3_code = c3.country_code
ORDER BY country_name)