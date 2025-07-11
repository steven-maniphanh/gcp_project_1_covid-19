WITH covid_19_merged AS(
  SELECT *,
  FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020`
  UNION ALL SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2021`
  UNION ALL SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2022`
  UNION ALL SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2023`
  UNION ALL SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2024`
  UNION ALL SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2025`)
  
SELECT *,
	DATE_DIFF(date, MIN(date) OVER (), DAY) AS day_since_day_1,
FROM covid_19_merged
ORDER BY date, country;


------------


