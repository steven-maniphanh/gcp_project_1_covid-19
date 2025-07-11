SELECT
  partition_id, total_rows
FROM `covid19-1-463112.looker_dataset_2_covid19_our_world.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'covid19_full_data_metrics';