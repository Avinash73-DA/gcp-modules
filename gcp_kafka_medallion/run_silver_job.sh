#!/bin/bash

## CONFIGURATION VARIABLES ##
PROJECT_ID=""
REGION="asia-south1"
BUCKET_NAME="gcs_kafka"
TEMPLATE_ID="silver-ingestion-workflow"
CLUSTER_NAME="ephemeral-spark-cluster"
ZONE=""


## JARS Config ##
JARS="gs://${BUCKET_NAME}/jars/delta-spark_2.12-3.0.0.jar,gs://${BUCKET_NAME}/jars/delta-storage-3.0.0.jar"

BRONZE_PATH="gs://gcs_kafka/bronze"
LANDING_PATH="gs://gcs_kafka/silver/"
CHECKPOINT_PATH="gs://gcs_kafka/checkpoints/silver_ck/"
PYTHON_FILE="gs://gcs_kafka/scripts/silver_ingestion.py"

# ## STEP 1: UPLOAD PYTHON SCRIPT ##
# echo "--- Uploading Python Script to GCS ---"
# gsutil cp bronze_ingestion.py ${PYTHON_FILE}

## STEP 2: CREATE EPHEMERAL CLUSTER ##
echo "--- Creating Ephemeral Dataproc Cluster ---"

# Delete existing template (suppress error if it doesn't exist)
gcloud dataproc workflow-templates delete ${TEMPLATE_ID} --region=${REGION} --quiet 2>/dev/null

# Create new empty template
gcloud dataproc workflow-templates create ${TEMPLATE_ID} --region=${REGION}

# Configure the Ephemeral 
# GCP will create this cluster ONLY when the job runs, and delete it immediately after
echo "--- Configuring Cluster Properties ---"
gcloud dataproc workflow-templates set-managed-cluster ${TEMPLATE_ID} \
    --region=${REGION} \
    --cluster-name=${CLUSTER_NAME} \
    --zone=${ZONE} \
    --single-node \
    --master-machine-type=e2-standard-4 \
    --master-boot-disk-size=50GB \
    --image-version=2.2-debian12 \
    --metadata "enable-oslogin=TRUE" \
    --properties="dataproc:pip.packages=delta-spark==3.0.0"

## STEP 3: ADD THE PYSPARK JOB TO THE TEMPLATE ##
echo "--- 3. Adding Job to Template ---"
gcloud dataproc workflow-templates add-job pyspark ${PYTHON_FILE} \
    --step-id="silver-ingestion-step" \
    --workflow-template=${TEMPLATE_ID} \
    --region=${REGION} \
    --jars=${JARS} \
    -- \
    --bronze_path=${BRONZE_PATH} \
    --silver_landing_layer=${LANDING_PATH} \
    --checkpoint=${CHECKPOINT_PATH}

## STEP 4: TRIGGER THE TEMPLATE ##
echo "--- 4. Triggering Workflow (Cluster will auto-create and auto-delete) ---"
gcloud dataproc workflow-templates instantiate ${TEMPLATE_ID} \
    --region=${REGION}
