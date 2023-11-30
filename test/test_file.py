from google.cloud import storage
from datetime import datetime, timedelta

def generate_signed_url(bucket_name, file_name, expiration_time=24):
    service_account_file_path = "./key.json"

    # Read the content of the service account key file
    with open(service_account_file_path, 'r') as file:
        service_account_json = file.read()
    print(service_account_json)

    client = storage.Client.from_service_account_json("./key.json")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Make the file publicly accessible
    blob.make_public()

    # Generate a signed URL
    expiration_time = datetime.utcnow() + timedelta(hours=expiration_time)
    return blob.generate_signed_url(expiration=expiration_time,response_disposition="attachment",)

# Example usage:
bucket_name = "incalsslab3"
file_name = "config.json"
signed_url = generate_signed_url(bucket_name, file_name)
print("Signed URL:", signed_url)