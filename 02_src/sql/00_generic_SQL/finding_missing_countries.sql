SELECT DISTINCT c1.country, c2.country_name, c2.alpha_3_code
FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_2020` as c1
LEFT JOIN `covid19-1-463112.raw_transverse_dataset.raw_country_dim` as c2
ON country_name = country
WHERE country_name IS NULL
ORDER BY country