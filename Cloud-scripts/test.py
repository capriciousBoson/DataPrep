import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage

def submit_job(project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name, input_gcs_path):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config. 'main_python_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": job_file_path,
            "args": [input_gcs_path]  # Pass the GCS path as an argument to the PySpark script
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    # Download the job output as bytes
    output_bytes = storage.Client().get_bucket(matches.group(1)).blob(f"{matches.group(2)}.000000000").download_as_bytes()

    # Upload the job output to the specified output bucket and blob
    storage.Client().bucket(output_bucket).blob(output_blob_name).upload_from_string(output_bytes.decode("utf-8"))

    print(f"Job finished successfully. Output uploaded to gs://{output_bucket}/{output_blob_name}")
#  new code used by me to test 

if __name__ == "__main__":
    project_id = "planar-depth-403402"
    region = "us-south1"
    cluster_name = "test"
    job_file_path = 'gs://inclasslab3/test_job.py'
    output_bucket = "final_5333"
    output_blob_name = "output.csv"
    input_gcs_path = "gs://landing_5333/inputtest.csv"
    submit_job(project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name, input_gcs_path)

# Ashish's driver code 
# if __name__ == "__main__":
#     project_id = 'dataprep-01-403222'
#     region = 'us-central1'
#     cluster_name = 'dataprep-cluster-1'
#     job_file_path = 'gs://dataproc-examples/pyspark/hello-world/hello-world.py'
#     output_bucket = "gs://dataprep-bucket-001/Processed-Data"
#     output_blob_name = "processed_data_009.csv"
#     input_gcs_path = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'
#     submit_job(project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name, input_gcs_path)
