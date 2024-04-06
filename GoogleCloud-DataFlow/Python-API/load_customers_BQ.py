# BELOW ARE THE LIBRARIES TO BE INSTALLED SEQUENTIALLY
# pip install pandas
# pip install apache_beam
# pip install apache_beam[gcp]
# pip install google-cloud-bigquery
# pip install google-cloud-storage
# TO AUTHENTICATE GOOGLE BUCKET FROM PYTHON USE BELOW CODE
# gcloud auth application-default login
# THIS SCRIPT IS USING python 3.8 version

from google.cloud import storage
import time
from google.cloud import bigquery

SERVICE_ACCOUNT_JSON = r'D:\GoogleCloud\Dataflow\iam_key\admiral-1409-b37ef309cbe2.json'
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

table_id = "admiral-1409.HRMS.customers"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("CustomerId", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("CustomerName", "STRING", mode="REQUIRED"),
    ],
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows =1, autodetect=True
)

sql_qry = """
        TRUNCATE TABLE admiral-1409.HRMS.customers
"""
qry_job = client.query(sql_qry)

file_path = r'D:\GoogleCloud\Dataflow\dataset\customers.csv'
source_file = open(file_path, "rb")
job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()

table = client.get_table(table_id)
print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema),table_id))

"""
python load_customers_BQ.py
"""