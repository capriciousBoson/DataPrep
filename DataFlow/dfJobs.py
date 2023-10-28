import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io import read_csv, to_csv
from apache_beam.dataframe import expressions as exp


PROJECT_ID='dataprep-01-403222'
JOB_NAME='dataprep-job-ash-07'
TEMP_DIR='gs://dataprep-bucket-001/temp'
STAGING_LOCATION ='gs://dataprep-bucket-001/staging'
REGION='us-south1'
SERVICE_ACCOUNT_EMAIL = "dataflow-gcs@dataprep-01-403222.iam.gserviceaccount.com"
KEY_FILE = 'DataFlow\dataprep-01-403222-12f89a955c05.json'
input_files = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'


beam_options = PipelineOptions(
    project = PROJECT_ID,
    region = REGION,
    temp_location = TEMP_DIR,
    staging_location = STAGING_LOCATION,
    service_account_email=SERVICE_ACCOUNT_EMAIL,
    service_account_key=KEY_FILE,
    job_name = JOB_NAME,
    #num_workers=2
    )
column_name='reads'
p = beam.Pipeline(DataflowRunner(), options=beam_options)

df = p | read_csv(input_files, splittable=False)
mean = df[column_name].mean()
std = df[column_name].std()

df[column_name] = (
            (df[column_name] - mean) / std
        )

df.to_csv('gs://dataprep-bucket-001/Processed-Data/processed_data_07.csv')

p.run().wait_until_finish()

