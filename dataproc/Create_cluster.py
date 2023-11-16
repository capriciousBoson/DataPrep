"""
This quickstart sample walks a user through creating a Cloud Dataproc
cluster, submitting a PySpark job from Google Cloud Storage to the
cluster, reading the output of the job and deleting the cluster, all
using the Python client library.

Usage:
    python quickstart.py --project_id <PROJECT_ID> --region <REGION> \
        --cluster_name <CLUSTER_NAME> --job_file_path <GCS_JOB_FILE_PATH>
"""

import argparse
import re

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def quickstart(project_id, region, cluster_name, job_file_path):
    # Create the cluster client.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 50},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 50},
            },
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    print("Cluster created successfully: {}".format(result.cluster_name))

    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config.
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

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )

    print(f"Job finished successfully: {output}")

    # Delete the cluster once the job has terminated.
    operation = cluster_client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    operation.result()

    print("Cluster {} successfully deleted.".format(cluster_name))


if __name__ == "__main__":
    project_id="planar-depth-403402"
    region="us-south1"
    cluster_name="test"
    job_file_path='gs://dataproc-examples/pyspark/hello-world/hello-world.py'
    quickstart(project_id, region, cluster_name,job_file_path)