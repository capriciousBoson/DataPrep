from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import re
from datetime import datetime, timedelta
import sys
import pyshorteners
print(sys.path)
from param import PROJECT_ID, JOB_NAME, TEMP_DIR, STAGING_LOCATION, REGION,SERVICE_ACCOUNT_EMAIL, KEY_FILE, input_files,JOB_FILE_PATH,CLUSTER_NAME,BUCKET_NAME

class DataprocJobView(APIView):
    def post(self, request, *args, **kwargs):

        project_id =PROJECT_ID
        region = REGION
        cluster_name = CLUSTER_NAME

        username = request.data.get('username')
        dataset_name = request.data.get("dataset_name")

        job_file_path = JOB_FILE_PATH

        bucket_name = BUCKET_NAME
        output_logs_blob_name = f"Processed-Data/logs/{username}_{dataset_name}_logs"

        input_gcs_path = f'gs://{bucket_name}/Raw-Data/{username}/{dataset_name}.csv'
        output_folder_path = f"Processed-Data/{username}/{dataset_name}_processed_data"
        output_file_extension='csv'

        gcp_service_creds = KEY_FILE
        


        if not all([project_id, region, cluster_name, job_file_path, bucket_name, output_logs_blob_name, input_gcs_path]):
            return Response({'error': 'Missing required parameters in the request.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            self.submit_job(project_id, region, cluster_name, job_file_path, bucket_name, output_logs_blob_name, input_gcs_path)
            print("generating url-----------------")
            download_url = self.generate_signed_url(bucket_name, output_folder_path, output_file_extension, gcp_service_creds)
            print(download_url)

            return Response({'message': 'Job submitted successfully.','download_url':download_url}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': f'Error submitting job: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    def submit_job(self, project_id, region, cluster_name, job_file_path, bucket_name, output_logs_blob_name, input_gcs_path):
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
        print("job creation \n",response)

        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output_bytes = storage.Client().get_bucket(matches.group(1)).blob(f"{matches.group(2)}.000000000").download_as_bytes()

        storage.Client().bucket(bucket_name).blob(output_logs_blob_name).upload_from_string(output_bytes.decode("utf-8"))

        print(f"Job finished successfully. Output uploaded to gs://{bucket_name}/{output_logs_blob_name}")

    def generate_signed_url(self, bucket_name, folder_name, file_extension, key_file_path, expiration_time_minutes=5):
        # Set up Google Cloud Storage client with explicit key file
        client = storage.Client.from_service_account_json(key_file_path)

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # List all blobs in the specified folder
        blobs = bucket.list_blobs(prefix=folder_name)

        # Filter blobs based on the file extension
        matching_blobs = [blob for blob in blobs if blob.name.endswith(f'.{file_extension}')]

        if not matching_blobs:
            raise ValueError(f'No matching files with extension {file_extension} found in folder {folder_name}')

        # Assuming the first matching blob is the desired file
        blob = matching_blobs[0]

        # Generate the expiration time for the signed URL
        expiration_time = datetime.utcnow() + timedelta(minutes=expiration_time_minutes)

        # Generate the signed URL
        signed_url = blob.generate_signed_url(
            version='v4',
            expiration=expiration_time,
            method='GET',
        )
        
        long_url =signed_url
        type_tiny = pyshorteners.Shortener()
        short_url = type_tiny.tinyurl.short(long_url)
        print("The Shortened URL is:",short_url)

        return short_url


    

    


