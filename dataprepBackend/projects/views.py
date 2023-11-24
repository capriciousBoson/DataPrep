from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import dfJobSerializer
#from .Scripts import dfJobs

import re
import json
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


class DataprocJobView(APIView):
    def post(self, request, *args, **kwargs):

        project_id = 'dataprep-01-403222'
        region = 'us-central1'
        cluster_name = 'dataprep-cluster-1'

        username = request.data.get('username')
        dataset_name = request.data.get("dataset_name")

        job_file_path = 'gs://dataprep-jobs/Dataproc-Jobs/dataProcJob.py'
        #job_file_path = r"C:\Users\avina\Desktop\CLASS_NOTES\5333-Cloud_Computing\Project\DataPrep\dataprepBackend\projects\Scripts\dataProcJob.py"
        job_file_path2 = 'gs://dataproc-examples/pyspark/hello-world/hello-world.py'
        output_bucket = "dataprep-bucket-001"
        output_blob_name = "Processed-Data/processed_data_9999.csv"
        #input_gcs_path = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'
        output_csv_path = "gs://dataprep-bucket-001/Processed-Data/processed_data_1230000.csv"
        input_gcs_path = f'gs://dataprep-bucket-001/Raw-Data/{username}/{dataset_name}.csv'
        #input_gcs_path = f'gs://dataprep-bucket-001/Raw-Data/{username}/{dataset_name}.csv'
        #output_path = f"gs://dataprep-bucket-001/Processed-Data/{username}/{dataset_name}_processed_12346.csv"

        transformations ={"reads":"mean_normalization"}


        if not all([project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name, input_gcs_path]):
            return Response({'error': 'Missing required parameters in the request.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            self.submit_job(project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name, input_gcs_path)
            return Response({'message': 'Job submitted successfully.'}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': f'Error submitting job: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def submit_job(self, project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name, input_gcs_path):
        job_client = dataproc.JobControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        job = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": job_file_path,
                "args": ["--input", input_gcs_path]
            },
        }

        operation = job_client.submit_job_as_operation(
            request={"project_id": project_id, "region": region, "job": job}
        )
        response = operation.result()

        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output_bytes = storage.Client().get_bucket(matches.group(1)).blob(f"{matches.group(2)}.000000000").download_as_bytes()

        storage.Client().bucket(output_bucket).blob(output_blob_name).upload_from_string(output_bytes.decode("utf-8"))

        print(f"Job finished successfully. Output uploaded to gs://{output_bucket}/{output_blob_name}")



