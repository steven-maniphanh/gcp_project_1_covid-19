#INPUT VARIABLES
FUNCTION_NAME="gcs-to-bq-v03"
RUNTIME="python310"
REGION="europe-west1"
PROJECT_ID="covid19-1-463112"
SOURCE_DIR="."
TRIGGER_EVENT="google.storage.object.finalize"
TRIGGER_RESOURCE="covid19-gcs-to-bq-cloud-function-bucket-1"
ENTRY_POINT="gcs_to_bq"
MEMORY="512MiB"
TIMEOUT="60s"
GCP_PROJECT_ID="$PROJECT_ID"
BQ_DATASET="dataset_2_covid19_our_world"
BQ_LOCATION="eu"

#ECHO
echo "Déploiement de la Cloud Function : $FUNCTION_NAME"
echo "Project : $PROJECT_ID | Region : $REGION | Runtime : $RUNTIME"
echo "Trigger : $TRIGGER_EVENT @ $TRIGGER_RESOURCE"
echo "Memory : $MEMORY | Timeout : $TIMEOUT"
echo "BQ_DATASET : $BQ_DATASET | BQ_LOCATION : $BQ_LOCATION"


#DEPLOY
gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --runtime="$RUNTIME" \
  --region="$REGION" \
  --trigger-event="$TRIGGER_EVENT" \
  --trigger-resource="$TRIGGER_RESOURCE" \
  --entry-point="$ENTRY_POINT" \
  --source="$SOURCE_DIR" \
  --memory="$MEMORY" \
  --timeout="$TIMEOUT" \
  --set-env-vars GCP_PROJECT_ID="$GCP_PROJECT_ID",BQ_DATASET="$BQ_DATASET",BQ_LOCATION="$BQ_LOCATION" \
  --project="$PROJECT_ID"