PROJECT_ID = "planar-depth-403402"
JOB_NAME = 'dataprep-job-ash-0087'
TEMP_DIR = 'gs://inclasslab3/temp'
STAGING_LOCATION = 'gs://inclasslab3/staging'
REGION = 'us-south1'
SERVICE_ACCOUNT_EMAIL = "project5333@planar-depth-403402.iam.gserviceaccount.com"
import os
KEY_FILE = os.path.join(os.path.dirname(__file__),'keys.json')
input_files = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'
JOB_FILE_PATH='gs://inclasslab3/dataProcJob.py'
CLUSTER_NAME = 'test'
BUCKET_NAME="inclasslab3"
