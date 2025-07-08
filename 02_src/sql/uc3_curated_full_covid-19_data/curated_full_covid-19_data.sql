WITH covid_data AS (
  SELECT
    c.country,
    c.region,
    c.date,
    EXTRACT(YEAR FROM c.date) AS covid_year,
    c.new_cases,
    c.total_cases,
    c.new_deaths,
    c.total_deaths,
    p.continent,
    p.area_km2,
    -- Dynamically pick the population for the covid year:
    CASE
      WHEN EXTRACT(YEAR FROM c.date) = 2020 THEN p.2020_population
      WHEN EXTRACT(YEAR FROM c.date) = 2021 THEN p.2021_population
      WHEN EXTRACT(YEAR FROM c.date) = 2022 THEN p.2022_population
      WHEN EXTRACT(YEAR FROM c.date) = 2023 THEN p.2023_population
      ELSE NULL
    END AS population
  FROM `covid19-1-463112.curated_dataset_2_covid19_our_world.covid19_cases_deaths_full_data` AS c
  JOIN `covid19-1-463112.staging_tranverse_dataset.full_world_population_data` AS p
  ON c.country = p.country
),

metrics AS (
  SELECT
    country,
    region,
    continent,
    date,
    total_cases,
    total_deaths,
    new_cases,
    new_deaths,
    population,
    area_km2,
    -- Metrics calculated per day
    SAFE_DIVIDE(total_cases, population) * 1000000 AS cases_per_million,
    SAFE_DIVIDE(total_deaths, population) * 1000000 AS deaths_per_million,
    SAFE_DIVIDE(total_deaths, total_cases) * 100 AS mortality_rate_pct,
    SAFE_DIVIDE(total_cases, population) * 100 AS infection_rate_pct,
    SAFE_DIVIDE(total_cases, area_km2) AS cases_per_km2,
    SAFE_DIVIDE(total_deaths, area_km2) AS deaths_per_km2
  FROM covid_data
)
SELECT *
FROM metrics
WHERE date ="2020-06-07"
ORDER BY country, date;