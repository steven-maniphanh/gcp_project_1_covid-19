{{ config(materialized='table') }}

WITH full_data AS (
    SELECT 
        w1.country_name, 
        w1.country_code, 
        w1.2020 AS pop_2020,
        w1.2021 AS pop_2021,
        w1.2022 AS pop_2022,
        w1.2023 AS pop_2023,
        w1.2024 AS pop_2024,
        c1.area_km2
    FROM `covid19-dbt-analytics-1.dev_country_raw.world_population` as w1
    INNER JOIN `covid19-dbt-analytics-1.dev_country_raw.country_geography` as c1
    ON w1.country_code = c1.country_code
    ORDER BY country_name)

    ,population_2020 AS (
        SELECT 
            country_name, 
            country_code, 
            2020 AS year, 
            full_data.pop_2020 AS population,
            area_km2,
            ROUND(full_data.pop_2020/area_km2, 1) AS density_km2
        FROM full_data)

    ,population_2021 AS (
        SELECT 
            country_name, 
            country_code, 
            2021 AS year, 
            full_data.pop_2021 AS population,
            area_km2,
            ROUND(full_data.pop_2021/area_km2, 1) AS density_km2
        FROM full_data)

    ,population_2022 AS (
        SELECT 
            country_name, 
            country_code, 
            2022 AS year, 
            full_data.pop_2022 AS population,
            area_km2,
            ROUND(full_data.pop_2022/area_km2, 1) AS density_km2
        FROM full_data)

    ,population_2023 AS (
        SELECT 
            country_name, 
            country_code, 
            2023 AS year, 
            full_data.pop_2023 AS population,
            area_km2,
            ROUND(full_data.pop_2023/area_km2, 1) AS density_km2
        FROM full_data)

    ,population_2024 AS (
        SELECT 
            country_name, 
            country_code, 
            2024 AS year, 
            full_data.pop_2024 AS population,
            area_km2,
            ROUND(full_data.pop_2024/area_km2, 1) AS density_km2
        FROM full_data)
 
SELECT 
    country_name,
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
UNION ALL 
SELECT * FROM population_2024
ORDER BY country_name , year