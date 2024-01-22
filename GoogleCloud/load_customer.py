# pip install google-cloud-bigquery
# pip install apache_beam
# pip install apache_beam[gcp]
# Resource : https://medium.com/@bhaveshpatil0078/loading-csv-file-from-gcs-to-bigquery-using-python-9c646d4e884f

from google.cloud import bigquery
import sys


PATH_TO_SA_KEY = 'path_to_your_service_account_key_file.json'
PROJECT = sys.argv[1]
DATASET = sys.argv[2]
TABLE = sys.argv[3]
TABLE_ID = f'{PROJECT}.{DATASET}.{TABLE}'
BQ_CLIENT = None
SCHEMA_LIST = []

def authenticate_with_gcp():
    '''Authenticate with BigQuery using service account key (not a recommended approach)'''
    global BQ_CLIENT
    BQ_CLIENT = bigquery.Client().from_service_account_json(PATH_TO_SA_KEY)

def set_schema():
    '''Get the schema of the target table in which the CSV file needs to be loaded'''
    hdw_schema = BQ_CLIENT.get_table(TABLE_ID)
    for field in hdw_schema.schema:
        SCHEMA_LIST.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))
    
def load_bq_table():
    '''Load the CSV file in BigQuery'''
    job_config = bigquery.LoadJobConfig()
    job_config.schema = SCHEMA_LIST
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = ','
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.null_marker = ''

    gcs_uri = f'gs://csv_artifacts/{TABLE}.csv'

    load_job = BQ_CLIENT.load_table_from_uri(
        gcs_uri, TABLE_ID, job_config=job_config
    )

    load_job.result() # waits for the load job to finish

    destination_table = BQ_CLIENT.get_table(TABLE_ID)

    print(f'Loaded {destination_table.num_rows} rows in table {TABLE}')

def main():
    authenticate_with_gcp()
    set_schema()
    load_bq_table()

if __name__ == '__main__':
    main()