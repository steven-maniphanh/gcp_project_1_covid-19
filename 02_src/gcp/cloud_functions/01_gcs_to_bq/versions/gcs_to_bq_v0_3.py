import os
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import bigquery


'''
Required Python packages (add to requirements.txt):

pandas
pandas-gbq
gcsfs
fsspec
google-cloud-bigquery
'''

def create_partitioned_clustered_table(client, dataset_id, table_name, schema, partition_field, cluster_fields, location):
    """
    Creates a BigQuery table with time-based partitioning and optional clustering.

    This function creates a table in the specified dataset and project with a defined schema,
    partitioned by a DATE field, and optionally clustered on one or more fields. If the table 
    already exists, it will not be recreated (safe to call multiple times).

    Args:
        client (google.cloud.bigquery.Client): The BigQuery client.
        dataset_id (str): The ID of the BigQuery dataset where the table will be created.
        table_name (str): The name of the BigQuery table to create.
        schema (List[google.cloud.bigquery.SchemaField]): The schema definition for the table.
        partition_field (str): The name of the column to use for time-based partitioning (must be of DATE or TIMESTAMP type).
        cluster_fields (List[str]): A list of column names to use for clustering (optional).
        location (str): The geographic location of the dataset (e.g., 'US', 'EU').

    Returns:
        None

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the table creation fails due to API or permission issues.
        Exception: For other unexpected errors.

    Example:
        >>> schema = [
        >>>     bigquery.SchemaField("date", "DATE"),
        >>>     bigquery.SchemaField("country", "STRING"),
        >>>     bigquery.SchemaField("cases", "INTEGER"),
        >>> ]
        >>> create_partitioned_clustered_table(
        >>>     client=bq_client,
        >>>     dataset_id="my_dataset",
        >>>     table_name="covid_data",
        >>>     schema=schema,
        >>>     partition_field="date",
        >>>     cluster_fields=["country"],
        >>>     location="EU"
        >>> )
    """

    table_id = f"{client.project}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)

    # Time-based partitioning on a DATE field
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field
    )

    # Clustering (optional)
    if cluster_fields:
        table.clustering_fields = cluster_fields

    try:
        client.create_table(table, exists_ok=True)
        print(f"Table {table_id} created or already exists.")
    except Exception as e:
        print(f"Error creating table {table_id}: {e}")


def gcs_to_bq(event, context):
    """
    Cloud Function to load a CSV file from Google Cloud Storage (GCS) into a BigQuery table 
    with partitioning and clustering. Also logs metadata and errors into dedicated BigQuery tables.

    This function is triggered by a new file being uploaded to a GCS bucket. It performs the following steps:
      1. Logs metadata about the GCS event to a BigQuery metadata table.
      2. Reads the uploaded CSV file from GCS.
      3. Creates a BigQuery table with time partitioning and clustering (if it doesn't exist).
      4. Loads the CSV data into the BigQuery table.
      5. Logs any errors that occur to a BigQuery error table.

    Environment Variables (to be set via deployment config):
        GCP_PROJECT_ID: GCP project ID.
        BQ_DATASET: Target BigQuery dataset.
        BQ_LOCATION: Geographic location of the BigQuery dataset (default: 'eu').

    Args:
        event (dict): Event payload from Cloud Storage. Must contain:
            - 'name': Name of the uploaded file.
            - 'bucket': Name of the bucket where the file was uploaded.
            - 'timeCreated': Timestamp when the file was created (optional).
            - 'updated': Timestamp when the file was last updated (optional).
        context (google.cloud.functions.Context): Metadata for the triggering event.
            - event_id: Unique event ID.
            - event_type: Type of event (e.g., google.storage.object.finalize).

    Returns:
        None

    Raises:
        Exceptions are caught and logged to a BigQuery error table. No exception is raised to Cloud Functions.

    Example:
        Uploading a file like `covid_cases.csv` to the bucket triggers:
            → A BigQuery table `dataset.covid_cases` will be created (if needed),
              partitioned by `date`, and clustered by `country`.
            → The file’s contents will be inserted into this table.
            → Metadata and errors (if any) are logged in `data_loading_metadata` and `data_loading_errors`.

    Dependencies:
        - pandas
        - pandas-gbq
        - google-cloud-bigquery
        - gcsfs (for reading GCS paths via pandas)
    """

    # Extract file name and table name
    file_name = event['name']
    table_name = file_name.split('.')[0]  # my_file.csv → table: my_file
    bucket_name = event['bucket']

    # Load from environment initialized from deploy.sh
    gcp_project_id = os.environ.get('GCP_PROJECT_ID')
    bq_dataset = os.environ.get('BQ_DATASET')
    bq_location = os.environ.get('BQ_LOCATION', 'eu')

    print(f"Loading file: {file_name} into table: {table_name}")

    # Step 1: Log event metadata
    metadata = {
        'Event_ID': context.event_id,
        'Event_type': context.event_type,
        'Bucket_name': bucket_name,
        'File_name': file_name,
        'Created': event.get('timeCreated'),
        'Updated': event.get('updated')
    }

    # Write event metadata into BQ table 'bq_dataset}.data_loading_metadata'
    try:
        df_metadata = pd.DataFrame([metadata])
        to_gbq(df_metadata,
               destination_table=f"{bq_dataset}.data_loading_metadata",
               project_id=gcp_project_id,
               if_exists='append',
               location=bq_location)
        print("Metadata logged.")
    except Exception as meta_error:
        print(f"Failed to log metadata: {meta_error}")

    try:
        # Step 2: Read CSV file from GCS
        file_path = f'gs://{bucket_name}/{file_name}'
        df_data = pd.read_csv(file_path)

        # Optionnal : Sort columns before loading to BigQuery
        df_data = df_data.sort_values(by=["country", "date"])

        # Step 3: Create BQ table with partitioning/clustering
        client = bigquery.Client()
        schema = [
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("new_cases", "INTEGER"),
            bigquery.SchemaField("total_cases", "INTEGER"),
            bigquery.SchemaField("new_deaths", "INTEGER"),
            bigquery.SchemaField("total_deaths", "INTEGER"),
            # Add more fields based on CSV file
        ]

        create_partitioned_clustered_table(
            client=client,
            dataset_id=bq_dataset,
            table_name=table_name,
            schema=schema,
            partition_field="date",
            cluster_fields=["country"],
            location=bq_location
        )

        # Step 4: Load CSV file data into BQ table 'bq_dataset.table_name'
        to_gbq(df_data,
               destination_table=f"{bq_dataset}.{table_name}",
               project_id=gcp_project_id,
               if_exists='append',  # preserve partitioned structure
               location=bq_location)
        print("Data successfully loaded to BigQuery.")

    except Exception as e:
        error_message = f"Error processing file {file_name}: {e}"
        print(error_message)

        # Step 5: Log error in BQ table 'bq.dataset.data_loading_errors'
        try:
            df_error = pd.DataFrame([{
                'file_name': file_name,
                'error_message': str(e)
            }])
            to_gbq(df_error,
                   destination_table=f"{bq_dataset}.data_loading_errors",
                   project_id=gcp_project_id,
                   if_exists='append',
                   location=bq_location)
            print("Error logged to BigQuery.")
        except Exception as error_log_error:
            print(f"Failed to log error to BigQuery: {error_log_error}")