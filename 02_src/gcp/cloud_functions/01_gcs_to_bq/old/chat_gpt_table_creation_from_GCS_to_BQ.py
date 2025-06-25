from google.cloud import bigquery

client = bigquery.Client()

table_id = "your_project.your_dataset.your_table"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date"  # Must be a DATE or TIMESTAMP column
    ),
    clustering_fields=["country", "region"]
)

uri = "gs://your-bucket/your-file.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)

load_job.result()  # Waits for the job to complete

print("✅ Loaded table with partitioning by date and clustering by country, region.")