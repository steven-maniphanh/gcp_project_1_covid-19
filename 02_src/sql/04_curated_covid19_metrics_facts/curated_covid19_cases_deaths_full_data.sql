CREATE OR REPLACE TABLE covid19-1-463112.curated_dataset_2_covid19_our_world.covid19_cases_deaths_full_data
PARTITION BY date
CLUSTER BY country AS

SELECT
  c1.country,
  c2.country_code,
  c1.region,
  c2.continent,
  c2.income_group, 
  EXTRACT(YEAR FROM date) AS year,
  FORMAT_DATE('%Y-%m', date) AS year_month,
  c1.date,
  c1.new_cases,
  c1.total_cases,
  c1.new_deaths,
  c1.total_deaths
FROM `covid19-1-463112.staging_dataset_2_covid19_our_world.covid19_cases_deaths_all_years` as c1
LEFT JOIN `covid19-1-463112.staging_tranverse_dataset.country_dim` as c2
ON c1.country = c2.country_name