# main.py
import base64
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import functions_framework
import re

project_id = 'lab-2-5333'
input_bucket = 'land_5333'
input_file = 'test.csv'
output_bucket = 'land_5333'
output_prefix = 'Out_'

def process_csv_file(element):
    # Your processing logic here
    # This example removes rows with NaN values, customize as needed
    cleaned_data = [field.strip() for field in element.split(',') if field.strip() != 'NaN']
    return ','.join(cleaned_data)


def generate_valid_job_name(file_name):
    # Remove invalid characters and replace them with underscores
    cleaned_name = re.sub(r'[^a-z0-9]', '_', file_name.lower())

    # Ensure the name starts with a letter
    if not cleaned_name[0].isalpha():
        cleaned_name = 'a' + cleaned_name[1:]

    # Ensure the name ends with a letter or number
    if not cleaned_name[-1].isalnum():
        cleaned_name = cleaned_name[:-1] + 'z'

    # Ensure the name is not empty
    if not cleaned_name:
        cleaned_name = 'default_job_name'

    # Remove underscores from the name
    cleaned_name = cleaned_name.replace('_', '')

    return cleaned_name

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def start_dataflow_process(cloud_event):
    data = cloud_event.data
    print(data)

    data = cloud_event.data
    print(data)

    # Assume 'data' is already a dictionary, no need to decode
    file_name = data['name']
    bucket_name = data['bucket']
    print(f"File: {file_name}, Bucket: {bucket_name}")
    # Set up Dataflow pipeline options
    job_name = generate_valid_job_name(file_name)
    pipeline_options = {
        'project': project_id,
        'runner': 'DataflowRunner',
        'staging_location': f'gs://{bucket_name}/staging',
        'temp_location': f'gs://{bucket_name}/temp',
        'job_name': job_name,
        'region': 'us-central1',  # Update with your desired Dataflow region
        }

    # Create Dataflow pipeline
    pipeline = beam.Pipeline(options=PipelineOptions.from_dictionary(pipeline_options))
    
    # Read CSV file
    csv_data = pipeline | 'ReadFromText' >> beam.io.ReadFromText(f'gs://{input_bucket}/{file_name}')

    # Process CSV data (remove NaN values)
    cleaned_data = csv_data | 'ProcessCSV' >> beam.Map(process_csv_file)

    # Write cleaned data to output bucket
    output_path = f'gs://{bucket_name}/{output_prefix}{file_name}'
    cleaned_data | 'WriteToText' >> beam.io.WriteToText(output_path)

    print("Pipeline starting...")
    
    # Run the pipeline
    result = pipeline.run()

    print("Pipeline started")

    # Wait for the job to finish
    result.wait_until_finish()

    print("Pipeline ended")


# Add debugging statements
print("Function loaded.")
