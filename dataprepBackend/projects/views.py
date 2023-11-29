from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import re
from datetime import datetime, timedelta
import os
import zipfile



class DataprocJobView(APIView):
    def generate_signed_url(self, bucket_name, folder_path, expiration_time=1):
        service_account_file_path = "confidential\dataprep-01-403222-5bedab8357fa.json"
        client = storage.Client.from_service_account_json(service_account_file_path)
        bucket = client.bucket(bucket_name)

        all_files = list(bucket.list_blobs(prefix=folder_path))

        # Exclude files with "success" in their names
        files = [file for file in all_files if "success" not in file.name.lower()]

        # Create a temporary directory to store the files
        temp_dir = "/tmp/folder_download/"
        os.makedirs(temp_dir, exist_ok=True)

        # Download each file to the temporary directory
        for file in files:
            if file.name!="_SUCCESS":
                file.download_to_filename(os.path.join(temp_dir, os.path.basename(file.name)))

        # Create a ZIP archive of the folder's contents
        zip_file_path = os.path.join(temp_dir, f"{folder_path.strip('/').replace('/', '_')}.zip")
        with zipfile.ZipFile(zip_file_path, 'w') as zipf:
            for file in files:
                zipf.write(os.path.join(temp_dir, os.path.basename(file.name)), file.name[len(folder_path):])

        # Make the ZIP file publicly accessible
        zip_blob = bucket.blob(zip_file_path.replace(temp_dir, ""))
        zip_blob.upload_from_filename(zip_file_path)
        zip_blob.make_public()

        # Generate a signed URL for the ZIP file
        expiration_time = datetime.utcnow() + timedelta(hours=expiration_time)
        signed_url = zip_blob.generate_signed_url(
            expiration=expiration_time,
            response_disposition="attachment",
        )
        return signed_url


    def post(self, request, *args, **kwargs):

        project_id = 'dataprep-01-403222'
        region = 'us-central1'
        cluster_name = 'dataprep-cluster-1'

        username = request.data.get('username')
        dataset_name = request.data.get("dataset_name")

        job_file_path = 'gs://dataprep-jobs/Dataproc-Jobs/dataProcJob.py'

        bucket_name = "dataprep-bucket-001"
        output_blob_name = f"Processed-Data/logs/{username}_{dataset_name}_logs"

        input_gcs_path = f'gs://{bucket_name}/Raw-Data/{username}/{dataset_name}.csv'

        output_path = f"Processed-Data/{username}/{dataset_name}_processed_data"


        if not all([project_id, region, cluster_name, job_file_path, bucket_name, output_blob_name, input_gcs_path]):
            return Response({'error': 'Missing required parameters in the request.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            #self.submit_job(project_id, region, cluster_name, job_file_path, bucket_name, output_blob_name, input_gcs_path)
            print("generating url-----------------")
            download_url = self.generate_signed_url(bucket_name, folder_path=output_path)
            print(download_url)

            return Response({'message': 'Job submitted successfully.'}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': f'Error submitting job: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    def submit_job(self, project_id, region, cluster_name, job_file_path, bucket_name, output_blob_name, input_gcs_path):
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

        storage.Client().bucket(bucket_name).blob(output_blob_name).upload_from_string(output_bytes.decode("utf-8"))

        print(f"Job finished successfully. Output uploaded to gs://{bucket_name}/{output_blob_name}")

    

    


