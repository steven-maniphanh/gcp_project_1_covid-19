CREATE OR REPLACE TABLE `covid19-1-463112.curated_dataset_2_covid19_our_world.covid19_cases_deaths_full_data`
PARTITION BY date
CLUSTER BY country AS
SELECT *
FROM `covid19-1-463112.staging_dataset_2_covid19_our_world.covid19_cases_deaths_*`
WHERE _TABLE_SUFFIX IN (
    '2020',
    '2021',
    '2022',
    '2023',
    '2024',
    '2025'
)