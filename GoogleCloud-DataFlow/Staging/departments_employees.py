from google.cloud import bigquery
from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import argparse
import csv
from apache_beam.runners import DataflowRunner, DirectRunner
import logging
import datetime

def _logging(elem):
    logging.info(elem)
    return elem

def main():
    parser = argparse.ArgumentParser(description='Load From BQ & Write To BQ')
    args, beam_args = parser.parse_known_args()

    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    beam_options = PipelineOptions(
    beam_args,
    runner = 'DirectRunner',
    project='admiral-1409',
    staging_location= 'gs://hrms-adm/utilities/staging',
    temp_location = 'gs://hrms-adm/utilities/temp',
    region = 'east-south1',
    service_account_email = 'dataflow@admiral-1409.iam.gserviceaccount.com'
    )

    qry = """
    SELECT
    d.deptid,
    d.DeptName,
    e.FName,
    e.LName
    FROM admiral-1409.HRMS.departments d INNER JOIN admiral-1409.HRMS.employees e
    ON d.deptid = e.deptid
    """

    p = beam.Pipeline(options=beam_options)
    
    logging.info('Reading From BQ and Loading into BQ')

    load_dept_employees_load = (p | "Read From BQ">>beam.io.ReadFromBigQuery(query= qry,
                           use_standard_sql = True) | "print valid data" >>beam.Map(_logging)
                           )
    
    load_dept_employees = (load_dept_employees_load
                           | "Write To BQ" >> beam.io.WriteToBigQuery(
                               table = 'admiral-1409.HRMS.DeptEmployees',
                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                               write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                           ))
    
    p.run().wait_until_finish()

if __name__=='__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Building Pipeline....')
    main()

"""
DirectRunner
python departments_employees.py
"""