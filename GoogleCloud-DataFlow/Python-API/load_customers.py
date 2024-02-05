# BELOW ARE THE LIBRARIES TO BE INSTALLED SEQUENTIALLY
# pip install pandas
# pip install apache_beam
# pip install apache_beam[gcp]
# pip install google-cloud-bigquery
# pip install google-cloud-storage
# TO AUTHENTICATE GOOGLE BUCKET FROM PYTHON USE BELOW CODE
# gcloud auth application-default login

import csv
import datetime
import logging
import apache_beam as beam
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

class CSVtoDict(beam.DoFn):
    """Converts line into dictionary"""
    def process(self, element, header):
        try:
            if len(element) == len(header):
                data = {header.strip(): val.strip() for header, val in zip(header, element)}
                data.update({"load_ts" : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                data.update({"load_user" : "PARIKSHEET"})
                print(data)
                return [data]
            else:
                logging.info("row contains bad data")
        except Exception as e:
            print(str(e))

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
                '--input',
                dest='input',
                type=str,
                default='',
                required=True,
                help='Input file to read. This can be a local file or a file in a Google Storage Bucket.')
        parser.add_argument(
                '--output',
                dest='output',
                type=str,
                default='',
                required=True,
                help='Output Big Query table where the data stored.')
        
def get_csv_reader(readable_file):
    import io
    # Open a channel to read the file from GCS
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)
    gcs_reader = csv.reader(io.TextIOWrapper(gcs_file))
    next(gcs_reader)
    print(gcs_reader)
    return gcs_reader

def dataflow(argv=None):
    #ile='sample.csv'
    #table = 'gp-ct-sbox-adv-dna:abcd.test_tabl_sumanta'
    parser = argparse.ArgumentParser()

    pipeline_options = PipelineOptions()
    my_options = pipeline_options.view_as(MyOptions)
    p = beam.Pipeline(options=pipeline_options)

    result = (p | 'Create from CSV' >> beam.Create([my_options.input])
                        | 'Flatten the CSV' >> beam.FlatMap(get_csv_reader)
                        | 'Converting from csv to dict' >> beam.ParDo(CSVtoDict(),['emp_id','f_name', 'l_name', 'dept_id' ,'joining_date'])
                        | 'Write entities into BigQuery' >> beam.io.WriteToBigQuery(
                                                                        # schema="SCHEMA_AUTODETECT",
                                                                        my_options.output,
                                                                        schema='emp_id:INTEGER, f_name:STRING , l_name:STRING , dept_id:INTEGER, joining_date:DATE, load_ts:TIMESTAMP, load_user:STRING',
                                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                        )
            )

    p.run()

if __name__=="__main__":
    dataflow()

# python load_customers.py --project admiral-1409 --region us-central1 --runner DirectRunner --staging_location gs://raw_batch_dataset/staging --temp_location gs://raw_batch_dataset/temp --service_account_email dataflow@admiral-1409.iam.gserviceaccount.com --max_num_workers 1 --setup_file ./setup.py --input gs://raw_batch_dataset/customers/customers.csv --output admiral-1409:HR.customer