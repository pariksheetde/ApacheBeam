# BELOW ARE THE LIBRARIES TO BE INSTALLED SEQUENTIALLY
# pip install pandas
# pip install apache_beam
# pip install apache_beam[gcp]
# pip install google-cloud-bigquery
# pip install google-cloud-storage
# TO AUTHENTICATE GOOGLE BUCKET FROM PYTHON USE BELOW CODE
# gcloud auth application-default login


from google.cloud import storage

gsclient = storage.Client()
gsclient.list_buckets()
print(gsclient.list_buckets())
bucket = list(gsclient.list_buckets())[0]