#!/bin/bash

## CONFIGURATION VARIABLES ##
PROJECT_ID=""
REGION="asia-south1"
BUCKET_NAME="gcs_kafka"
TEMPLATE_ID="bronze-ingestion-workflow"
CLUSTER_NAME="ephemeral-spark-cluster"
ZONE="asia-south1-a"


## JARS Config ##
JARS="gs://${BUCKET_NAME}/jars/delta-spark_2.12-3.0.0.jar,gs://${BUCKET_NAME}/jars/delta-storage-3.0.0.jar,gs://${BUCKET_NAME}/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,gs://${BUCKET_NAME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,gs://${BUCKET_NAME}/jars/kafka-clients-3.4.1.jar,gs://${BUCKET_NAME}/jars/commons-pool2-2.11.1.jar"

KAFKA_IP=""
LANDING_PATH="gs://gcs_kafka/bronze/"
CHECKPOINT_PATH="gs://gcs_kafka/checkpoints/bronze_ck/"
PYTHON_FILE="gs://gcs_kafka/scripts/bronze_ingestion.py"

# ## STEP 1: UPLOAD PYTHON SCRIPT ##
# echo "--- Uploading Python Script to GCS ---"
# gsutil cp bronze_ingestion.py ${PYTHON_FILE}

## STEP 2: CREATE EPHEMERAL CLUSTER ##
echo "--- Creating Ephemeral Dataproc Cluster ---"

# Delete existing template (suppress error if it doesn't exist)
gcloud dataproc workflow-templates delete ${TEMPLATE_ID} --region=${REGION} --quiet 2>/dev/null

# Create new empty template
gcloud dataproc workflow-templates create ${TEMPLATE_ID} --region=${REGION}

# Configure the Ephemeral Cluster
# GCP will create this cluster ONLY when the job runs, and delete it immediately after
gcloud dataproc workflow-templates set-managed-cluster ${TEMPLATE_ID} \
    --region=${REGION} \
    --cluster-name=${CLUSTER_NAME} \
    --zone=${ZONE} \
    --single-node \
    --master-machine-type=e2-standard-4 \
    --master-boot-disk-size=50GB \
    --image-version=2.2-debian12 \
    --metadata "enable-oslogin=TRUE"

## STEP 3: ADD THE PYSPARK JOB TO THE TEMPLATE ##
echo "--- 3. Adding Job to Template ---"
gcloud dataproc workflow-templates add-job pyspark ${PYTHON_FILE} \
    --step-id="bronze-ingestion-step" \
    --workflow-template=${TEMPLATE_ID} \
    --region=${REGION} \
    --jars=${JARS} \
    -- \
    --EXTERNAL_IP=${KAFKA_IP} \
    --landing_path=${LANDING_PATH} \
    --checkpoint=${CHECKPOINT_PATH}

## STEP 4: TRIGGER THE TEMPLATE ##
echo "--- 4. Triggering Workflow (Cluster will auto-create and auto-delete) ---"
gcloud dataproc workflow-templates instantiate ${TEMPLATE_ID} \
    --region=${REGION}
