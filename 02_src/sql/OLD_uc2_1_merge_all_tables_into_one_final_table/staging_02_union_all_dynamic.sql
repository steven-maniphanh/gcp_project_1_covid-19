CREATE OR REPLACE TABLE `covid19-1-463112.staging_dataset_2_covid19_our_world.covid19_cases_deaths_2020`
PARTITION BY date
CLUSTER BY country AS
SELECT *
FROM `covid19-1-463112.raw_dataset_2_covid19_our_world.cases_deaths_2020_*`
WHERE _TABLE_SUFFIX IN (
  'africa', 
  'americas', 
  'eastern_mediterranean', 
  'europe', 
  'south_east_asia', 
  'western_pacific'
)

-----

CREATE OR REPLACE TABLE dataset_2_covid19_our_world.cvid19_cases_deaths_final
PARTITION BY date
CLUSTER BY country AS
SELECT * FROM (
  SELECT table_name
  FROM `covid19-1-463112.dataset_2_covid19_our_world.__TABLES__`
  WHERE table_id LIKE 'cases_deaths_%'
),
UNNEST(REGEXP_EXTRACT_ALL(table_name, r'(.*)')) AS dynamic_query



-----

DECLARE years ARRAY<INT64> DEFAULT [2020, 2021, 2022, 2023, 2024, 2025];
DECLARE regions ARRAY<STRING> DEFAULT ['africa', 'americas', 'eastern_mediterranean', 'europe', 'south_east_asia', 'western_pacific'];
DECLARE year INT64;
DECLARE region STRING;
DECLARE query STRING DEFAULT '';

SET query = '''
CREATE OR REPLACE TABLE `covid19-1-463112.staging_dataset_2_covid19_our_world.covid19_cases_deaths_all`
PARTITION BY date
CLUSTER BY country AS
''';

-- Loop over years and regions to build UNION ALLs
FOR year IN UNNEST(years) DO
  FOR region IN UNNEST(regions) DO
    SET query = CONCAT(
      query,
      'SELECT * FROM `covid19-1-463112.dataset_2_covid19_our_world.cases_deaths_', CAST(year AS STRING), '_', region, '` UNION ALL\n'
    );
  END FOR;
END FOR;

-- Remove final UNION ALL
SET query = LEFT(query, LENGTH(query) - 10); -- Remove last UNION ALL

EXECUTE IMMEDIATE query;

