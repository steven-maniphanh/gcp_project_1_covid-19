CREATE OR REPLACE TABLE dataset_2_covid19_our_world.cvid19_cases_deaths_final
PARTITION BY DATE(date)
CLUSTER BY country AS
SELECT * FROM (
  SELECT table_name
  FROM `covid19-1-463112.dataset_2_covid19_our_world.__TABLES__`
  WHERE table_id LIKE 'cases_deaths_%'
),
UNNEST(REGEXP_EXTRACT_ALL(table_name, r'(.*)')) AS dynamic_query