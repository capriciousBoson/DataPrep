from google.cloud import storage
from google.auth.transport.requests import Request
from google.auth.credentials import Credentials
from datetime import datetime, timedelta, timezone



def delete_files_older_than(bucket_name, folder_path, hours_threshold):
    
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = client.list_blobs(bucket_name,prefix=folder_path)

    # Print the list of blobs
    print(f"Blobs in folder '{folder_path}':")
    blobs = bucket.list_blobs(prefix='Raw-Data/')

    # Iterate through each file or folder in the 'raw-data' folder
    for blob in blobs:
        print(blob.name)
        # Get the creation/update time of the file or folder
        update_time = blob.updated  # Use blob.time_created for creation time
        update_time=update_time.replace(tzinfo=None)
        # Calculate the time difference
        current_time = datetime.utcnow()
        current_time=current_time.replace(tzinfo=None)
        age = current_time - update_time
        print(age)

        # If the file or folder is more than one hour old, delete it
        if age.total_seconds() > 3600:
            print(f"Deleting {blob.name} from {bucket_name}")
            if blob.content_type == 'application/x-www-form-urlencoded':
                # It's a folder, recursively delete its content
                delete_folder_contents(bucket, blob.name)
            else:
                # It's a file, delete it
                blob.delete()
                print(f"{blob.name} deleted successfully.")
        else:
            print(f"{blob.name} is not more than one hour old. No deletion needed.")

def delete_folder_contents(bucket, folder_path):
    blobs = bucket.list_blobs(prefix=folder_path)
    for blob in blobs:
        blob.delete()
        print(f"{blob.name} deleted successfully.")

@functions_framework.cloud_event
def auto_delete(data):
    # Replace with your GCS bucket name, folder path, threshold, and service account key path
    gcs_bucket_name = data['bucket']
    folder_path = "Raw-Data/"
    hours_threshold = 1
    # service_account_key_path = "../dataprepBackend/keys.json"

    # Delete files older than 1 hour in the specified folder
    delete_files_older_than(gcs_bucket_name, folder_path, hours_threshold)
