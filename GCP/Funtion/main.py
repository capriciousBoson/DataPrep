# main.py
import base64
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

project_id = 'lab-2-5333'
input_bucket = 'test-5333'
input_file = 'test.csv'
output_bucket = 'test-5333'
output_prefix = 'Out_'

def process_csv_file(element):
    # Your processing logic here
    # This example removes rows with NaN values, customize as needed
    cleaned_data = [field.strip() for field in element.split(',') if field.strip() != 'NaN']
    return ','.join(cleaned_data)

def start_dataflow_process(data, context):
    if 'data' in data:
        message = base64.b64decode(data['data']).decode('utf-8')
        file_data = json.loads(message)
        file_name = file_data['name']

        project = project_id
        output_bucket_function = output_bucket
        output_path = f'{output_bucket_function}/{output_prefix}{file_name}'
        job_name = f'dataflow-job-{file_name}'

        pipeline_options = {
            'project': project,
            'runner': 'DataflowRunner',
            'staging_location': 'gs://path/to/your/staging/location',
            'temp_location': 'gs://path/to/your/temp/location',
            'job_name': job_name,
        }

        pipeline = beam.Pipeline(options=PipelineOptions.from_dictionary(pipeline_options))
        
        # Read CSV file
        csv_data = pipeline | 'ReadFromText' >> beam.io.ReadFromText(f'gs://{input_bucket}/{file_name}')

        # Process CSV data (remove NaN values)
        cleaned_data = csv_data | 'ProcessCSV' >> beam.Map(process_csv_file)

        # Write cleaned data to output bucket
        cleaned_data | 'WriteToText' >> beam.io.WriteToText(output_path)

        # Run the pipeline
        result = pipeline.run()

        # Wait for the job to finish
        result.wait_until_finish()
