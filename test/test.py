import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage

def submit_job(project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": job_file_path},
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

if __name__ == "__main__":
    project_id = "planar-depth-403402"
    region = "us-south1"
    cluster_name = "test"
    job_file_path = 'gs://dataproc-examples/pyspark/hello-world/hello-world.py'
    output_bucket = "your_output_bucket_name"
    output_blob_name = "your_output_blob_name"
    submit_job(project_id, region, cluster_name, job_file_path, output_bucket, output_blob_name)
