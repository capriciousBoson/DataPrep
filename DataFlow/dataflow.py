import apache_beam as beam
from apache_beam.transforms.window import GlobalWindows
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


SERVICE_ACCOUNT_EMAIL = "dataflow-gcs@dataprep-01-403222.iam.gserviceaccount.com"
KEY_FILE = 'DataFlow\dataprep-01-403222-12f89a955c05.json'

# Define a function to read CSV files from a GCS bucket.
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

# Define a function to process and save the data.
class ProcessAndSave(beam.DoFn):
    def process(self, element):
        df = element
    
        # Remove NaN values
        #df = df.dropna()
        # Mean normalization
  

        # Step 1: Replace NaN values with the mean
        #imputer = SimpleImputer(strategy='mean')
        #df['Value1'] = imputer.fit_transform(df[['Value1']])

        # Step 2: Perform mean normalization
        scaler = StandardScaler()
        df['reads'] = scaler.fit_transform(df[['reads']])

        # Step 3: Perform one-hot encoding
        enc = OneHotEncoder(handle_unknown='ignore')
        # passing bridge-types-cat column (label encoded values of bridge_types)
        enc_df = pd.DataFrame(enc.fit_transform(df[['experiment_type']]).toarray())
        # merge with main df bridge_df on key values
        df = df.join(enc_df)
    

        #output_path ='gs://dataprep-bucket-001/Processed-Data/processed_dataset.csv'
        #encoded_df.to_csv(output_path, index=False)
      
        #output_path ='gs://dataprep-bucket-001/Processed-Data/processed_dataset.csv'
        #df.to_csv(output_path, index=False)

        yield df

# Define the Dataflow pipeline.
def run():
    # Define PipelineOptions and specify the service account and key file.
    options = PipelineOptions([
        '--project=dataprep-01-403222',
        '--runner=DataflowRunner',
        '--temp_location=gs://dataprep-bucket-001/tmp',
        #'--temp_location=C:/Users/avina\Desktop/CLASS_NOTES/5333-Cloud_Computing/Project/DataPrep/DataFlow/temp',
        '--staging_location=gs://dataprep-bucket-001/staging',
        #'--setup_file=./setup.py',  # Add any necessary setup file if required.
        '--service_account_email={}'.format(SERVICE_ACCOUNT_EMAIL),
        '--service_account_key={}'.format(KEY_FILE)
    ])
    with beam.Pipeline() as p:
        input_files = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'

        # Read CSV files from the GCS bucket
        csv_data = p | 'ReadCSV' >> beam.io.ReadFromText(input_files)

        # Set a default global windowing strategy for batch processing.
        csv_data = csv_data | 'FixedGlobalWindow' >> beam.WindowInto(GlobalWindows())


        # Process and save the data
        processed_data  = csv_data | 'ProcessAndSave' >> beam.ParDo(ProcessAndSave())

        # Save data
        processed_data | "Write Processed Data" >> beam.io.WriteToText('gs://dataprep-bucket-001/Processed-Data/processed_dataset.csv')

if __name__ == '__main__':
    run()
