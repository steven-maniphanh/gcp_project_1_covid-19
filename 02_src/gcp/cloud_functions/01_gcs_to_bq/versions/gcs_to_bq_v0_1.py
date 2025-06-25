import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

'''
Python Dependencies to be installed

gcsfs
fsspec
pandas
pandas-gbq

'''

def gcs_to_bq(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    lst = []
    file_name = event['name']
    table_name = file_name.split('.')[0]

    #Input Variables
    gcp_project_id = 'covid19-1-463112'
    bq_dataset = 'gcp_dataeng_demos'

    # Write Event/File metadata details store in a dictionnary
    dct={
         'Event_ID':context.event_id,
         'Event_type':context.event_type,
         'Bucket_name':event['bucket'],
         'File_name':event['name'],
         'Created':event['timeCreated'],
         'Updated':event['updated']
        }
    
    lst.append(dct)

    # Write metadata of event and context into a BQ table
    df_metadata = pd.DataFrame.from_records(lst)
    df_metadata.to_gbq(destination_table= bq_dataset + '.data_loading_metadata', 
                        project_id= gcp_project_id, 
                        if_exists='append',
                        location='us')
    
    # Write csv file to a datset.table in BQ
    df_data = pd.read_csv('gs://' + event['bucket'] + '/' + file_name)

    df_data.to_gbq(destination_table= bq_dataset + '.' + table_name, 
                        project_id= gcp_project_id, 
                        if_exists='append',
                        location='us')