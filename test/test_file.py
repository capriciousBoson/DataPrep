from google.cloud import storage
from datetime import datetime, timedelta
import zipfile
import os
def generate_signed_url(bucket_name, folder_path, expiration_time=24):
    service_account_file_path = r"C:\Users\avina\Desktop\CLASS_NOTES\5333-Cloud_Computing\Project\DataPrep\dataprepBackend\confidential\dataprep-01-403222-5bedab8357fa.json"

    # Read the content of the service account key file
    # with open(service_account_file_path, 'r') as file:
    #     service_account_json = file.read()
    # print(service_account_json)

    client = storage.Client.from_service_account_json(service_account_file_path)
    bucket = client.bucket(bucket_name)

    all_files = list(bucket.list_blobs(prefix=folder_path))
    print(all_files)

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

# Example usage:
bucket_name = "dataprep-bucket-001"
folder_path = "Processed-Data/ash101/subset_dataset_processed_data"
signed_url = generate_signed_url(bucket_name, folder_path)
print("Signed URL:", signed_url)