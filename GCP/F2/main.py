import base64
from datetime import datetime
from google.cloud import storage
from googleapiclient.discovery import build
import os

def process_csv_files(event, context):
    # Extract the bucket and file information from the Cloud Storage event
    bucket_name = event['bucket']
    file_name = event['name']
    file_path = f'gs://{bucket_name}/{file_name}'

    # Set your Dataflow and Cloud Storage details
    project_id = os.environ.get('GCP_PROJECT', 'YOUR_PROJECT_ID')
    template_path = 'gs://{}/templates/RemoveNaNValuesTemplate'.format(bucket_name)
    output_path = f'gs://{bucket_name}/output'

    # Launch Dataflow job using the template
    launch_dataflow_job(project_id, bucket_name, template_path, file_path, output_path)

def launch_dataflow_job(project_id, bucket_name, template_path, input_file, output_path):
    service = build('dataflow', 'v1b3')

    # Set a unique job name based on the current timestamp
    job_name = f'dataflow-job-{datetime.now().strftime("%Y%m%d%H%M%S")}'

    # Build the Dataflow job parameters
    body = {
        'jobName': job_name,
        'parameters': {
            'input': input_file,
            'output': output_path,
        },
        'environment': {
            'tempLocation': f'gs://{bucket_name}/dataflow/temp',
        }
    }

    # Launch the Dataflow job
    request = service.projects().templates().launch(projectId=project_id, gcsPath=template_path, body=body)
    response = request.execute()

    return response
