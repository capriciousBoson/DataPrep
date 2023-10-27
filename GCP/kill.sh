#!/bin/bash

# Get active job IDs
active_jobs=$(gcloud dataflow jobs list --status=active --format='value(JOB_ID)')

# Cancel each active job
for job_id in $active_jobs; do
  gcloud dataflow jobs cancel $job_id
done
