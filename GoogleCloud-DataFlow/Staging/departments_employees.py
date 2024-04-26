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
# DESCRIPTION : THIS SCRIPT WILL READ FROM GOOGLE BIGQUERY AND LOAD INTO GOOGLE BIGQUERY
# METHOD 1 : USING DirectRunner
# METHOD 2 : USING Dataflow 

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
    # runner = 'DataflowRunner',
    # use_public_ips = False,
    # subnetwork = 'https://www.googleapis.com/compute/v1/projects/admiral-1409/regions/asia-south1/subnetworks/dataflow-svps',
    # machine_type = 'n2-custom-6-3072',
    project='admiral-1409',
    staging_location= 'gs://hrms-adm/utilities/staging',
    temp_location = 'gs://hrms-adm/utilities/temp',
    region = 'east-south1',
    service_account_email = 'dataflow@admiral-1409.iam.gserviceaccount.com'
    )

    dept_emp_associates = """
    SELECT
    e.EmpId,
    e.FName,
    e.LName,
    d.deptid AS DeptId,
    d.DeptName,
    current_timestamp() AS LoadTS
    FROM admiral-1409.HRMS.departments d INNER JOIN admiral-1409.HRMS.employees e
    ON d.deptid = e.deptid
    """

    p = beam.Pipeline(options=beam_options)
    
    logging.info('Reading From BQ and Loading into BQ')

    load_dept_employees_load = (p | "Read From BQ">>beam.io.ReadFromBigQuery(query= dept_emp_associates,
                           use_standard_sql = True) | "print valid data" >>beam.Map(_logging)
                           )
    
    load_dept_employees = (load_dept_employees_load
                           | "Write To BQ" >> beam.io.WriteToBigQuery(
                               table = 'admiral-1409.Staging.DeptEmpAssociates',
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

"""
python departments_employees.py --machine_type n2-custom-6-3072 --no_use_public_ips --subnetwork https://www.googleapis.com/compute/v1/projects/admiral-1409/regions/asia-south1/subnetworks/dataflow-svps --job_name deptemp --runner DataflowRunner
"""
