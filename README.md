# DataPrep

#Funtions
!!important note make sure the source  and staging buckets are different to make sure the function is not triggered for the staging files created

option 1: go to folder functions  and create a function based on a trigger  and local code using the below command
Use the below command to start the function

main.py has the function code
requiremts.txt has the dependencies 

gcloud functions deploy start_dataflow_process     --runtime python38     --trigger-resource land_5333 --trigger-event google.storage.object.finalize

Option 2: 
create a bucket and use process using a function option  and configure the function as python 3.8  and copy and paste the funtion/main.py, funtion/setup.py, and fution/requirements.py in the respective file in the inline editor 


the entry poit for the funtion is start_dataflow_process
