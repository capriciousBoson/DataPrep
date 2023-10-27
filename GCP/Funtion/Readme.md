
Use the below command to start the funtion
main.py has the funtion code
requiremts.txt has the dependencies 
gcloud functions deploy start_dataflow_process  --runtime python310  --trigger-resource test-5333     --trigger-event google.storage.object.finalize


gcloud functions deploy start_dataflow_process     --runtime python39     --trigger-resource land_5333 --trigger-event google.storage.object.finalize


<!-- create  a dat flopw template  -->

python Nan.py --runner DataflowRunner --project lab-2-5333 --staging_location gs://land_5333/staging --temp_location gs://land_5333/temp --region YOUR_REGION --template_location gs://land_5333/Code/Remove_nan_template

python Nan.py --runner DataflowRunner --project lab-2-5333 --staging_location gs://land_5333/staging --temp_location gs://land_5333/temp --region YOUR_REGION us-central1 gs://land_5333/Code/Remove_nan_template --input gs://land_5333/*.csv --output gs://land_5333/out
