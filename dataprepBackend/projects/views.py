from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import dfJobSerializer
#from .Scripts import dfJobs

import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


import apache_beam as beam
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io import read_csv, to_csv
from apache_beam.dataframe import expressions as exp

class dataProcApi(APIView):
    global dataproc_project_idproject_id, region, cluster_name

    dataproc_project_id = 'dataprep-01-403222'
    region = 'us-central1'
    cluster_name = 'dataprep-cluster-1'

    def post(self, request, *args, **kwargs):
        global dataproc_project_id, region, cluster_name
        serializer = dfJobSerializer(data=request.data)
        if serializer.is_valid():
            metadata = serializer.validated_data
            # Extract metadata values as needed
            project_name =  metadata.get('project_name')
            project_id =    metadata.get('project_id')
            dataset_name =  metadata.get('dataset_name')
            userame =       metadata.get('userame')

            try:
                # Your script execution logic here, using metadata values if needed
                print(project_name, project_id ,dataset_name ,userame )
                job_client = dataproc.JobControllerClient(
                    client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
                input_gcs_path='gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'
                job = {
                            "placement": {"cluster_name": cluster_name},
                            "pyspark_job": {
                                "main_python_file_uri": 'gs://dataproc-examples/pyspark/hello-world/hello-world.py',
                                "args": ["--input", input_gcs_path]  # Pass the GCS path as an argument to the PySpark script
                            },
                        }
                operation = job_client.submit_job_as_operation(
                                request={"project_id": dataproc_project_id, "region": region, "job": job})
                response = operation.result()
                #gcloud dataproc jobs submit pyspark --cluster=dataprep-01-403222 gs://dataproc-examples/pyspark/hello-world/hello-world.py 

                # Dataproc job output gets saved to the Google Cloud Storage bucket
                # allocated to the job. Use a regex to obtain the bucket and blob info.
                matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

                # Download the job output as bytes
                output_bytes = storage.Client().get_bucket(matches.group(1)).blob(f"{matches.group(2)}.000000000").download_as_bytes()

                # Upload the job output to the specified output bucket and blob

                output_bucket = "gs://dataprep-bucket-001/Processed-Data"
                output_blob_name = "processed_data_009.csv"
                storage.Client().bucket(output_bucket).blob(output_blob_name).upload_from_string(output_bytes.decode("utf-8"))

                print(f"Job finished successfully. Output uploaded to gs://{output_bucket}/{output_blob_name}")

                ####################################################################
                print("Job done !!!!!!!")
                return Response({'success': True, 'result': "here should be result!!!!!"})
            except Exception as e:
                return Response({'success': False, 'message': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return Response({'success': False, 'message': 'Invalid metadata'}, status=status.HTTP_400_BAD_REQUEST)

        


class dfJobsapi(APIView):
    def post(self, request, *args, **kwargs):
        serializer = dfJobSerializer(data=request.data)
        if serializer.is_valid():
            metadata = serializer.validated_data
            # Extract metadata values as needed
            project_name =  metadata.get('project_name')
            project_id =    metadata.get('project_id')
            dataset_name =  metadata.get('dataset_name')
            userame =       metadata.get('userame')

            try:
                # Your script execution logic here, using metadata values if needed
                print(project_name, project_id ,dataset_name ,userame )

                ###################################################################
                PROJECT_ID='dataprep-01-403222'
                JOB_NAME='dataprep-job-ash-009'
                TEMP_DIR='gs://dataprep-bucket-001/temp'
                STAGING_LOCATION ='gs://dataprep-bucket-001/staging'
                REGION='us-south1'
                SERVICE_ACCOUNT_EMAIL = "dataflow-gcs@dataprep-01-403222.iam.gserviceaccount.com"
                KEY_FILE = 'DataFlow\dataprep-01-403222-12f89a955c05.json'
                input_files = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'

                print("Params read !!!!!")
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
                print("beam options created!!!!")
                column_name='reads'
                p = beam.Pipeline(DataflowRunner(), options=beam_options)

                df = p | read_csv(input_files, splittable=False)
                print("csv read !!!!!")
                #mean = df[column_name].mean()
                #std = df[column_name].std()

                #if std != 0:
                    #df[column_name] = (df[column_name] - mean) / std

                print("processing done!!!!")
                df.to_csv('gs://dataprep-bucket-001/Processed-Data/processed_data_07.csv')

                p.run().wait_until_finish()

                ####################################################################
                print("Job done !!!!!!!")
                return Response({'success': True, 'result': "here should be result!!!!!"})
            except Exception as e:
                return Response({'success': False, 'message': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return Response({'success': False, 'message': 'Invalid metadata'}, status=status.HTTP_400_BAD_REQUEST)

