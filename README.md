# DataPrep

#Funtions
<!-- important note make sure the source  and staging buckets are different to make sure the funtion is not triggered for the staging files created -->
<!-- option 1 : go to folder funtions  and create funtion based on trigger  and local code using below command
Use the below command to start the funtion
main.py has the funtion code
requiremts.txt has the dependencies 
-->
gcloud functions deploy start_dataflow_process     --runtime python39     --trigger-resource land_5333 --trigger-event google.storage.object.finalize
<!-- option2: create a bucket and use process unsing a funtion option  and configure the funtion as python 3.8  and copy paste the funtion/main.py , funtion/setup.py and fution/requirements.py in the respective file in the inline editor -->
