# BELOW ARE THE LIBRARIES TO BE INSTALLED SEQUENTIALLY
# pip install pandas
# pip install apache_beam
# pip install apache_beam[gcp]
# pip install google-cloud-bigquery
# pip install google-cloud-storage
# TO AUTHENTICATE GOOGLE BUCKET FROM PYTHON USE BELOW CODE
# gcloud auth application-default login
# THIS SCRIPT IS USING python 3.8 version

# AUTHOR : PARIKSHEET DE
# DATE : 06-APR-2024
# DESCRIPTION : THIS SCRIPT WILL LOAD CSV FILE FROM LOCAL DRIVE INTO GOOGLE BIGQUERY

import pandas as pd
from google.cloud import storage
import datetime
from google.cloud import bigquery

SERVICE_ACCOUNT_JSON = r'D:\DataSet\GoogleCloud\Dataflow\iam_key\admiral-1409-b37ef309cbe2.json'
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

# Read the CSV file into a DataFrame
csv_file_path = "D:\DataSet\GoogleCloud\Dataflow\dataset\departments.csv"
df = pd.read_csv(csv_file_path)

# Add additional columns to the DataFrame
# For example:
df['LoadTS'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df['User'] = 'ORACLE'

# Initialize BigQuery client
client = bigquery.Client()

# Define BigQuery dataset and table names
dataset_id = 'HRMS'
table_id = 'departments'

# Construct a BigQuery job config
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("DeptID", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("DeptName", "STRING", mode="REQUIRED"),
    ],
    # Specify the job configuration to append data to the table
    write_disposition="WRITE_TRUNCATE"
)

# Load DataFrame into BigQuery
job = client.load_table_from_dataframe(df, f"{dataset_id}.{table_id}", job_config=job_config)

# Wait for the job to complete
job.result()

print(f"Loaded {len(df)} rows into {dataset_id}.{table_id}")


"""
python load_departments_BQ.py
"""