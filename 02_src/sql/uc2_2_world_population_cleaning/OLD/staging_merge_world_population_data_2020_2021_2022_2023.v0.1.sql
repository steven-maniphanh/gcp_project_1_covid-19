CREATE OR REPLACE TABLE `covid19-1-463112.staging_tranverse_dataset.full_world_population_data` AS(
    SELECT 
        w1.country, 
        w1.country_code, 
        w1.continent, 
        w1.`2020_population`,
        w2.`2021_population`,
        w1.`2022_population`, 
        w1.`2023_population`,
        w1.area_km2, 
        w1.density_km2, 
        w1.`growth_rate_%`,
        w1.`world_percentage_%`
    FROM `covid19-1-463112.raw_transverse_dataset.world_population_data_2020_2022_2023` as w1
    INNER JOIN `covid19-1-463112.raw_transverse_dataset.world_population_data_2021_only` as w2
    ON w1.country_code = w2.country_code
    ORDER BY country)