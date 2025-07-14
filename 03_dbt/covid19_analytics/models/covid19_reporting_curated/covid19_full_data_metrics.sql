{{ config(
    materialized='table',
    partition_by={
        "field": "date",             
        "data_type": "date"           
    },
    cluster_by=['country']
) }}


WITH CT1 AS (
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
  FROM {{ref('covid19_cases_deaths_all_years_region')}} as c1
  LEFT JOIN {{ref('country_dim')}} as c2
  ON c1.country = c2.country_name
)

 ,CTE2 AS (
  SELECT
    c1.country,
    c1.country_code,
    c1.region,
    c1.continent,
    c1.income_group,
    c1.date,
    EXTRACT(YEAR FROM date) AS year,
    FORMAT_DATE('%Y-%m', date) AS year_month,
    c1.new_cases,
    c1.total_cases,
    c1.new_deaths,
    c1.total_deaths,
    c2.population,
    c2.area_km2,
    c2.density_km2,
    SUM(c1.new_cases) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_new_cases,
    SUM(c1.new_deaths) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_new_deaths,
    SUM(c1.new_cases) OVER (PARTITION BY c1.country ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS biweekly_new_cases,
    SUM(c1.new_deaths) OVER (PARTITION BY c1.country ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS biweekly_new_deaths,
    SUM(c1.new_cases) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS monthly_new_cases,
    SUM(c1.new_deaths) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS monthly_new_deaths,
    AVG(c1.new_cases) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_avg_new_cases,
    AVG(c1.new_deaths) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_avg_new_deaths,
    AVG(c1.new_cases) OVER (PARTITION BY c1.country ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS biweekly_avg_new_cases,
    AVG(c1.new_deaths) OVER (PARTITION BY c1.country ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS biweekly_avg_new_deaths,
    AVG(c1.new_cases) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS monthly_avg_new_cases,
    AVG(c1.new_deaths) OVER (PARTITION BY c1.country ORDER BY c1.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS monthly_avg_new_deaths
  FROM CT1 AS c1
  LEFT JOIN {{ref('country_population_facts')}} AS c2
    ON c1.country_code = c2.country_code
  WHERE c2.year = EXTRACT(YEAR FROM date)
)

SELECT 
  country,
  country_code,
  region,
  continent,
  income_group,
  year,
  year_month,
  date,
  new_cases AS daily_new_cases,
  total_cases,
  new_deaths AS daily_new_deaths,
  total_deaths,
  population,
  area_km2,
  density_km2,
  -- Metrics
  weekly_new_cases,
  weekly_new_deaths,
  biweekly_new_cases,
  biweekly_new_deaths,
  monthly_new_cases,
  monthly_new_deaths,
  weekly_avg_new_cases,
  weekly_avg_new_deaths,
  biweekly_avg_new_cases,
  biweekly_avg_new_deaths,
  monthly_avg_new_cases,
  monthly_avg_new_deaths,
  ROUND(SAFE_DIVIDE(new_cases, population), 6) * 1000000 AS daily_new_cases_per_million,
  ROUND(SAFE_DIVIDE(new_deaths, population), 6) * 1000000 AS daily_new_deaths_per_million,
  ROUND(SAFE_DIVIDE(weekly_new_cases, population), 6) * 1000000 AS weekly_new_cases_per_million,
  ROUND(SAFE_DIVIDE(weekly_new_deaths, population), 6) * 1000000 AS weekly_new_deaths_per_million,
  ROUND(SAFE_DIVIDE(biweekly_new_cases, population), 6) * 1000000 AS biweekly_new_cases_per_million,
  ROUND(SAFE_DIVIDE(biweekly_new_deaths, population), 6) * 1000000 AS biweekly_new_deaths_per_million,
  ROUND(SAFE_DIVIDE(monthly_new_cases, population), 6) * 1000000 AS monthly_new_cases_per_million,
  ROUND(SAFE_DIVIDE(monthly_new_deaths, population), 6) * 1000000 AS monthly_new_deaths_per_million,
  ROUND(SAFE_DIVIDE(total_cases, population), 6) * 1000000 AS total_cases_per_million,
  ROUND(SAFE_DIVIDE(total_deaths, population), 6) * 1000000 AS total_deaths_per_million,
  ROUND(SAFE_DIVIDE(total_cases, population), 4) * 100 AS total_infection_rate_pct,
  ROUND(SAFE_DIVIDE(total_deaths, total_cases), 4) * 100 AS total_mortality_rate_pct,
  ROUND(SAFE_DIVIDE(weekly_new_cases, population), 4) * 100 AS weekly_infection_rate_pct,
  ROUND(SAFE_DIVIDE(weekly_new_deaths, weekly_new_cases), 4) * 100 AS weekly_mortality_rate_pct,
  ROUND(SAFE_DIVIDE(biweekly_new_cases, population), 4) * 100 AS biweekly_infection_rate_pct,
  ROUND(SAFE_DIVIDE(biweekly_new_deaths, biweekly_new_cases), 4) * 100 AS biweekly_mortality_rate_pct,
  ROUND(SAFE_DIVIDE(monthly_new_cases, population), 4) * 100 AS monthly_infection_rate_pct,
  ROUND(SAFE_DIVIDE(monthly_new_deaths, monthly_new_cases), 4) * 100 AS monthly_mortality_rate_pct
FROM CTE2