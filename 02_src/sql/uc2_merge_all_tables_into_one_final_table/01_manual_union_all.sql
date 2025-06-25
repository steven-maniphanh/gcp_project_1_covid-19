CREATE OR REPLACE TABLE dataset_2_covid19_our_world.covid19_cases_deaths_final
PARTITION BY date
CLUSTER BY country AS
SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020_africa`
UNION ALL
SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020_americas`
UNION ALL
SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020_eastern_mediterranean`
UNION ALL
SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020_europe`
UNION ALL
SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020_south_east_asia`
UNION ALL
SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020_western_pacific`;