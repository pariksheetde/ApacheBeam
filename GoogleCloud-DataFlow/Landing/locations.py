# BELOW ARE THE LIBRARIES TO BE INSTALLED SEQUENTIALLY
# pip install pandas
# pip install apache_beam
# pip install apache_beam[gcp]
# pip install google-cloud-bigquery
# pip install google-cloud-storage
# TO AUTHENTICATE GOOGLE BUCKET FROM PYTHON USE BELOW CODE
# gcloud auth application-default login
# THIS SCRIPT IS USING python 3.8 version

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
import argparse
import csv
import datetime
import logging

def _logging(elem):
    logging.info(elem)
    return elem

class CSVtoDict(beam.DoFn):
    def process(self, element, header):
        import datetime
        try:
            element = element.split(",")
            logging.info(len(element))
            if len(element) == len(header):
                data = {header.strip(): val.strip() for header, val in zip(header, element)}
                data.update({"LoadTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                data.update({"SourceSystem": "ORACLE"})
                logging.info(str(data))
                print(data)
                return[data]
            else:
                logging.info("Contains bad Data")
        except Exception as err:
            logging.info(str(err))

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--inputBucket',
            dest = 'inputBucket',
            type = str,
            default = "",
            required = True,
            help = "Input GCS bucket to fetch CSV File"
        )
def get_csv_reader(readable_file):
    import apache_beam as beam
    import io
    import csv
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)
    gcs_reader = csv.reader(io.TextIOWrapper(gcs_file))
    next(gcs_reader)
    logging.info(gcs_reader)
    return gcs_reader
    
def dataflow(argv=None):
    pipeline_options = PipelineOptions()
    my_options = pipeline_options.view_as(MyOptions)
    print(my_options.inputBucket)
    pl = beam.Pipeline(options=pipeline_options)

    results = (pl
                | 'Create From CSV' >> beam.io.ReadFromText(my_options.inputBucket, skip_header_lines=1)
                | 'Print' >> beam.Map(_logging)
                | 'Converting From CSV to Dict' >> beam.ParDo(CSVtoDict(), ['location_id', 'location_name'])
                | 'Write To BQ' >> beam.io.WriteToBigQuery('admiral-1409:HR.locations',
                                                            schema = 'location_id:INTEGER, location_name:STRING, load_ts:TIMESTAMP, user:STRING',
                                                            write_disposition= beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                 )
    pl.run().wait_until_finish()

if __name__=='__main__':
    # logging.getLogger().setLevel(logging.info)
    dataflow()

# python locations.py --project admiral-1409 --region us-central1 --runner DirectRunner --service_account_email dataflow@admiral-1409.iam.gserviceaccount.com --temp_location gs://rms-adm/hrms-adm-utility/temp --staging_location gs://hrms-adm/hrms-adm-utility/staging --inputBucket gs://hrms-adm/src/locations.csv