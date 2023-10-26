
Use the below command to start the funtion
main.py has the funtion code
requiremts.txt has the dependencies 
gcloud functions deploy start_dataflow_process  --runtime python310  --trigger-resource test-5333     --trigger-event google.storage.object.finalize
