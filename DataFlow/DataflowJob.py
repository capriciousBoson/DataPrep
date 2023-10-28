import apache_beam as beam
from apache_beam.dataframe.io import read_csv, to_csv
from apache_beam.transforms.window import GlobalWindows
from apache_beam.dataframe import expressions as exp
#import pandas as pd
#from sklearn.preprocessing import StandardScaler, OneHotEncoder
#from sklearn.impute import SimpleImputer
#from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions
#import csv

SERVICE_ACCOUNT_EMAIL = "dataflow-gcs@dataprep-01-403222.iam.gserviceaccount.com"
KEY_FILE = 'DataFlow\dataprep-01-403222-12f89a955c05.json'

#class ProcessAndSave(beam.DoFn):
def process(df, column):

    #df = element

    # Remove NaN values
    #df = df.dropna()
    # Mean normalization

    column_name= column

    mean = df |"agg-1" >> beam.Map(lambda row: row[column_name]) | "find-mean" >> beam.Combine(beam.combiners.Mean())
    std = df | "agg-2" >> beam.Map(lambda row: row[column_name]) | "find-std" >> beam.Combine(beam.combiners.StandardDeviation())

    # Normalize the column.
    df = df | "normalize" >> beam.Map(lambda row: (row[column_name] - mean) / std)
    

    return df

# Define the Dataflow pipeline.
def run():
    # Define PipelineOptions and specify the service account and key file.

    beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='dataprep-01-403222',
    job_name='dataprep-job-ash-01',
    temp_location='gs://dataprep-bucket-001/temp',
    staging_location='gs://dataprep-bucket-001/staging',
    region='us-south1',
    service_account_email=SERVICE_ACCOUNT_EMAIL,
    service_account_key=KEY_FILE)

    with beam.Pipeline(options=beam_options) as p:
        input_files = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'

        # Read CSV files from the GCS bucket
        dataframe = p | 'ReadCSV' >> read_csv(input_files)

        # Set a default global windowing strategy for batch processing.
        dataframe = dataframe | 'FixedGlobalWindow' >> beam.WindowInto(GlobalWindows())

        #mean = dataframe | beam.Map(lambda row: row['reads']).mean()
        #std = dataframe | beam.Map(lambda row: row['reads']).std()

        #df = dataframe | beam.Map(lambda row: (row['reads'] - mean) / std)


        print("done  windowing..............")
        # Process and save the data
        #processed_data  = dataframe | 'ProcessAndSave' >> beam.ParDo(ProcessAndSave())
        processed_data = process(dataframe, 'reads')

        print("done _ processing the data.....................")

        # Save data
        processed_data | "Write Processed Data" >> to_csv('gs://dataprep-bucket-001/Processed-Data/processed_dataset.csv')

        print("Saved data to bucket....................................")

if __name__ == '__main__':
    run()