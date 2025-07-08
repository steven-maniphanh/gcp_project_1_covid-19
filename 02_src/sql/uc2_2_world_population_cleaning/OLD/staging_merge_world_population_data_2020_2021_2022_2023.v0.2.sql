CREATE OR REPLACE TABLE `covid19-1-463112.staging_tranverse_dataset.country_population_facts` AS (

WITH full_data AS (
    SELECT 
        w1.country, 
        w1.country_code, 
        w1.continent, 
        w1.`2020_population`,
        w2.`2021_population`,
        w1.`2022_population`, 
        w1.`2023_population`,
        c1.area_km2
    FROM `covid19-1-463112.raw_transverse_dataset.world_population_data_2020_2022_2023` as w1
    INNER JOIN `covid19-1-463112.raw_transverse_dataset.world_population_data_2021_only` as w2
    ON w1.country_code = w2.country_code
    INNER JOIN covid19-1-463112.staging_tranverse_dataset.country_dim as c1
    ON w1.country_code = c1.alpha_3_code
    ORDER BY country)

    ,population_2020 AS (
        SELECT 
            country, 
            country_code, 
            2020 AS year, 
            full_data.2020_population AS population,
            area_km2,
            ROUND(full_data.2020_population/area_km2, 1) AS density_km2
        FROM full_data)

    ,population_2021 AS (
        SELECT 
            country, 
            country_code, 
            2021 AS year, 
            full_data.2021_population AS population,
            area_km2,
            ROUND(full_data.2021_population/area_km2, 1) AS density_km2
        FROM full_data)

    ,population_2022 AS (
        SELECT 
            country, 
            country_code, 
            2022 AS year, 
            full_data.2022_population AS population,
            area_km2,
            ROUND(full_data.2022_population/area_km2, 1) AS density_km2
        FROM full_data)

    ,population_2023 AS (
        SELECT 
            country, 
            country_code, 
            2023 AS year, 
            full_data.2023_population AS population,
            area_km2,
            ROUND(full_data.2023_population/area_km2, 1) AS density_km2
        FROM full_data)

 
SELECT 
    country AS country_name, 
    country_code, 
    year, 
    population,
    area_km2,
    density_km2
FROM population_2020
UNION ALL 
SELECT * FROM population_2021 
UNION ALL 
SELECT * FROM population_2022 
UNION ALL 
SELECT * FROM population_2023 
ORDER BY country_name , year
)