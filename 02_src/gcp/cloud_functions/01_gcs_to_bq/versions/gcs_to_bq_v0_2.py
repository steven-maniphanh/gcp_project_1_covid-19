import pandas as pd
import os
from pandas_gbq import to_gbq #instead of from pandas.io import gbq
from google.cloud import bigquery

'''
Python Dependencies (add to requirements.txt):

pandas
pandas-gbq
gcsfs
fsspec
google-cloud-bigquery

'''

def gcs_to_bq(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # Extract file name and table name
    file_name = event['name']
    table_name = file_name.split('.')[0]  # e.g. my_file.csv → table: my_file

    # Load env variables passed at deployment
    #gcp_project_id = 'covid19-1-463112'
    #bq_dataset = 'dataset_2_covid19_our_world'
    #bq_location = 'eu'
    gcp_project_id = os.environ.get('GCP_PROJECT_ID')
    bq_dataset = os.environ.get('BQ_DATASET')
    bq_location = os.environ.get('BQ_LOCATION', 'eu')  # fallback if not set
    print(f' gcp_project_id: {gcp_project_id} \n bq_dataset : {bq_dataset} \n bq_location : {bq_location}')

    # Capture metadata about the event
    metadata = {
        'Event_ID': context.event_id,
        'Event_type': context.event_type,
        'Bucket_name': event['bucket'],
        'File_name': event['name'],
        'Created': event.get('timeCreated'),
        'Updated': event.get('updated')
    }

    # Write metadata to BigQuery
    df_metadata = pd.DataFrame([metadata])
    to_gbq(df_metadata,
       destination_table=f"{bq_dataset}.data_loading_metadata",
       project_id=gcp_project_id,
       if_exists='append',
       location=bq_location)

    # Try loading the CSV and writing it to BigQuery
    try:
        file_path = f'gs://{event["bucket"]}/{file_name}'
        df_data = pd.read_csv(file_path)

        to_gbq(df_data,
          destination_table=f"{bq_dataset}.{table_name}",
          project_id=gcp_project_id,
          if_exists='replace',
          location=bq_location)
        
    except Exception as e:
        error_message = f"Error processing file {file_name}: {e}"
        print(error_message)

    # Write the error to a separate table for tracking
    df_error = pd.DataFrame([{'file_name': file_name, 'error_message': str(e)}])
    try:
        to_gbq(df_error,
                destination_table=f"{bq_dataset}.data_loading_errors",
                project_id=gcp_project_id,
                if_exists='append',
                location=bq_location)
    except Exception as error_log_error:
        print(f"Error logging error to BigQuery: {error_log_error}")