from google.cloud import storage

# Replace 'your-project-id' with your actual project ID
project_id = 'lab-2-5333'

client = storage.Client(project=project_id)
bucket = client.get_bucket('5333test')
blob = bucket.blob('test.csv')
blob.download_to_filename('/tmp/test.csv')