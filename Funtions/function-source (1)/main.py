# main.py
import base64
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import functions_framework
import re

project_id = 'planar-depth-403402'
input_bucket = 'landing_5333'
input_file = 'test.csv'
output_bucket = 'final_5333'
output_prefix = 'Out_'

def process_csv_file(element):
    cleaned_data = [field.strip() for field in element.split(',') if field.strip() != 'NaN']
    return ','.join(cleaned_data)

def generate_valid_job_name(file_name):
    cleaned_name = re.sub(r'[^a-z0-9]', '_', file_name.lower())
    if not cleaned_name[0].isalpha():
        cleaned_name = 'a' + cleaned_name[1:]
    if not cleaned_name[-1].isalnum():
        cleaned_name = cleaned_name[:-1] + 'z'
    if not cleaned_name:
        cleaned_name = 'default_job_name'
    cleaned_name = cleaned_name.replace('_', '')
    return cleaned_name

@functions_framework.cloud_event
def start_dataflow_process(cloud_event):
    data = cloud_event.data
    file_name = data['name']
    bucket_name = data['bucket']
    
    # Check if the keyword 'input' is present in the file name
    if 'input' not in file_name.lower():
        print(f"File {file_name} does not contain the keyword 'input'. Exiting.")
        return
    
    print(f"File: {file_name}, Bucket: {bucket_name}")

    job_name = generate_valid_job_name(file_name)
    pipeline_options = {
        'project': project_id,
        'runner': 'DataflowRunner',
        'staging_location': f'gs://{output_bucket}/staging',
        'temp_location': f'gs://{output_bucket}/temp',
        'job_name': job_name,
        'region': 'us-central1',
        'setup_file': './setup.py',
        'save_main_session': True
    }

    pipeline = beam.Pipeline(options=PipelineOptions.from_dictionary(pipeline_options))
    csv_data = pipeline | 'ReadFromText' >> beam.io.ReadFromText(f'gs://{input_bucket}/{file_name}')
    cleaned_data = csv_data | 'ProcessCSV' >> beam.Map(process_csv_file)
    output_path = f'gs://{bucket_name}/{output_prefix}{file_name}'
    cleaned_data | 'WriteToText' >> beam.io.WriteToText(output_path)

    print("Pipeline starting...")
    result = pipeline.run()
    print("Pipeline started")
    result.wait_until_finish()
    print("Pipeline ended")

# Add debugging statements
print("Function loaded.")
