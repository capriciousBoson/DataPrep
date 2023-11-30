from google.cloud import storage
from datetime import datetime, timedelta

def generate_signed_url(bucket_name, folder_name, file_extension, key_file_path, expiration_time_minutes=5):
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

    return signed_url

# Example usage:
bucket_name = "dataprep-bucket-001"
folder_name = "Processed-Data/ash101/subset_dataset_processed_data"
file_extension = 'csv'
key_file_path = r"C:\Users\avina\Desktop\CLASS_NOTES\5333-Cloud_Computing\Project\DataPrep\dataprepBackend\confidential\dataprep-01-403222-5bedab8357fa.json"

signed_url = generate_signed_url(bucket_name, folder_name, file_extension, key_file_path)

print(f'Signed URL: {signed_url}')
