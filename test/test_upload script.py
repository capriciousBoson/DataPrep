from google.cloud import storage
import json
import os



def upload_to_gcloud(data: dict):
    """
    this function take a dictionary as input and uploads
    it in a google cloud storage bucket
    """          
    service_account_file_path = "./key.json"

    # Read the content of the service account key file
    with open(service_account_file_path, 'r') as file:
        service_account_json = file.read()
    print(service_account_json)
    ## your service-account credentials as JSON file
    # os.environ['GOOLE_APPLICATION_CREDENTAILS'] = service_account_json
    # credentials = service_account.Credentials.from_service_account_file(service_account_file_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)

    
    ## instane of the storage client
    storage_client = storage.Client.from_service_account_json("./key.json")

    ## instance of a bucket in your google cloud storage
    bucket = storage_client.get_bucket("incalsslab3")
    
    ## if you want to create a new file 
    blob = bucket.blob("config.json")

    ## if there already exists a file
    
    # blob = bucket.get_blob("config.json")
    
    print(blob)
    ## uploading data using upload_from_string method
    ## json.dumps() serializes a dictionary object as string
    blob.upload_from_string(json.dumps(data),content_type='application/json')
    print("uploaded")

config={'key':"value"}
upload_to_gcloud(config)