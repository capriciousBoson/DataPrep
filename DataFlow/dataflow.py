import apache_beam as beam
from apache_beam.transforms.window import GlobalWindows
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import csv


SERVICE_ACCOUNT_EMAIL = "dataflow-gcs@dataprep-01-403222.iam.gserviceaccount.com"
KEY_FILE = 'DataFlow\dataprep-01-403222-12f89a955c05.json'

# Define a function to read CSV files from a GCS bucket.
"""
class ReadCSV(beam.DoFn):
    def process(self, element):
        # Assuming 'element' is the GCS file path.
        client = storage.Client()
        bucket = client.get_bucket('gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv')
        blob = bucket.get_blob(element)
        if blob is not None:
            content = blob.download_as_text()
            df = pd.read_csv(pd.compat.StringIO(content))
            yield df


def to_dataframe(row):
    # Use a CSV reader to extract column names from the first row
    if not hasattr(to_dataframe, 'header'):
        to_dataframe.header = next(csv.reader([row]))
    # Split the row into data
    data = row.split(',')
    # Create a DataFrame with extracted column names
    df = pd.DataFrame([data], columns=to_dataframe.header)
    yield df


def parse_csv_line(line):
    # Split the CSV line into a list of values
    values = line.split(',')
    
    # Create a Pandas DataFrame from the list of values
    df = pd.DataFrame([values])
    #print("---------------------------")
    #print(df.shape)
    
    return df
"""

# Define a function to process and save the data.
class ProcessAndSave(beam.DoFn):
    def process(self, element):

        df = element
    
        # Remove NaN values
        df = df.dropna()
        # Mean normalization
  

        # Step 1: Replace NaN values with the mean
        #imputer = SimpleImputer(strategy='mean')
        #df['Value1'] = imputer.fit_transform(df[['Value1']])

        # Step 2: Perform mean normalization
        #scaler = StandardScaler()
        #df['reads'] = scaler.fit_transform(df[['reads']])

        # Step 3: Perform one-hot encoding
        #enc = OneHotEncoder(handle_unknown='ignore')
        # passing bridge-types-cat column (label encoded values of bridge_types)
        #enc_df = pd.DataFrame(enc.fit_transform(df[['experiment_type']]).toarray())
        # merge with main df bridge_df on key values
        #df = df.join(enc_df)
    

        #output_path ='gs://dataprep-bucket-001/Processed-Data/processed_dataset.csv'
        #encoded_df.to_csv(output_path, index=False)
      
        #output_path ='gs://dataprep-bucket-001/Processed-Data/processed_dataset.csv'
        #df.to_csv(output_path, index=False)

        yield df

# Define the Dataflow pipeline.
def run():
    # Define PipelineOptions and specify the service account and key file.
    """
    options2 = PipelineOptions([
        '--project=dataprep-01-403222',
        '--',
        #'--runner=DataflowRunner',
        '--runner=DataflowRunner',
        '--temp_location=gs://dataprep-bucket-001/tmp',
        #'--temp_location=C:/Users/avina\Desktop/CLASS_NOTES/5333-Cloud_Computing/Project/DataPrep/DataFlow/temp',
        '--staging_location=gs://dataprep-bucket-001/staging',
        #'--setup_file=./setup.py',  # Add any necessary setup file if required.
        '--service_account_email={}'.format(SERVICE_ACCOUNT_EMAIL),
        '--service_account_key={}'.format(KEY_FILE)
    ])
    """
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
        csv_data = p | 'ReadCSV' >> beam.dataframe.io.read_csv(input_files)
        #csv_data = p | 'ReadCSV' >> beam.ParDo(ReadCSV())

        #csv_data = csv_data | 'ToDataFrame' >> beam.Map(to_dataframe)

        #dataframe = text_data | beam.Map(parse_csv_line)
        #print("done  converting to dataframe ..............")
        #print(dataframe.shape)

        # Set a default global windowing strategy for batch processing.
        #dataframe = dataframe | 'FixedGlobalWindow' >> beam.WindowInto(GlobalWindows())

        print("done  windowing..............")
        # Process and save the data
        processed_data  = csv_data | 'ProcessAndSave' >> beam.ParDo(ProcessAndSave())

        print("done _ processing the data.....................")

        # Save data
        processed_data | "Write Processed Data" >> beam.io.WriteToText('gs://dataprep-bucket-001/Processed-Data/processed_dataset.csv')

        print("Saved data to bucket....................................")

if __name__ == '__main__':
    run()
