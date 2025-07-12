{{ config(
  materialized='table',
  partition_by={'field': 'date', 'data_type': 'date'},
  cluster_by=['country']
) }}

SELECT * FROM {{ ref('raw_covid_2021') }}
UNION ALL
SELECT * FROM {{ ref('raw_covid_2022') }}