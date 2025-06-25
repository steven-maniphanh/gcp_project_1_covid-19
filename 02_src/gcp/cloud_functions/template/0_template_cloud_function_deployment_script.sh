#!/bin/bash

# =================================
# 🎯 Configuration générale
# =================================

# 🔧 Nom de la fonction à déployer
FUNCTION_NAME="hello_gcs"

# 📦 Runtime Python (ex: python310, python39, etc.)
RUNTIME="python310"

# 🌍 Région GCP (ex: europe-west1, us-central1)
REGION="europe-west1"

# 🏢 ID du projet GCP
PROJECT_ID="covid19-1-463112" 
#or : export PROJECT_ID=$(gcloud config get-value project)

# 📁 Répertoire source de la fonction (contenant main.py et requirements.txt)
SOURCE_DIR="."

# ⚡ Type d’événement déclencheur (GCS ou Pub/Sub, etc.)
TRIGGER_EVENT="google.storage.object.finalize"

# 📦 Ressource déclencheuse (nom du bucket ou topic Pub/Sub)
TRIGGER_RESOURCE="nom-du-bucket-gcs-demo"

# 🚀 Nom de la fonction dans le script Python (ex: def hello_gcs(event, context):)
ENTRY_POINT="hello_gcs"

# 🧠 Mémoire allouée (ex: 128MiB, 256MiB, 512MiB, 1GiB)
MEMORY="512MiB"

# ⏱️ Timeout d'exécution maximum (ex: 60s, 120s, 300s)
TIMEOUT="60s"

# 🌱 Variables d’environnement (accessibles via os.environ.get())
ENV_VARS="GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=gcp_dataeng_demos,BQ_LOCATION=us" #ENV_VARS=KEY1=value1,KEY2=value2,...

# =================================
# 🚀 Lancement du déploiement
# =================================

echo "🔧 Déploiement de la Cloud Function : $FUNCTION_NAME"
echo "🌍 Projet : $PROJECT_ID | Région : $REGION | Runtime : $RUNTIME"
echo "⚡ Trigger : $TRIGGER_EVENT @ $TRIGGER_RESOURCE"
echo "🧠 Mémoire : $MEMORY | ⏱️ Timeout : $TIMEOUT"
echo "🌱 Env Vars : $ENV_VARS"

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
  --set-env-vars="$ENV_VARS" \ #--set-env-vars=KEY1=value1,KEY2=value2,...
  --project="$PROJECT_ID"

# ✅ Vérification du succès
if [ $? -eq 0 ]; then
  echo "✅ Déploiement réussi !"
else
  echo "❌ Échec du déploiement."
fi


# =====================
# Informations compléementaires
# =====================

"""📦 Liste des événements Cloud Storage disponibles pour déclencher une Cloud Function (Cloud Functions Gen2)

1️⃣ google.storage.object.finalize
    🔹 Déclenché lorsqu’un objet est **créé ou remplacé** dans un bucket GCS.
    🔹 Utilisé pour détecter la mise en ligne ou la mise à jour d’un fichier.
    🔹 Équivalent : "nouveau fichier" ou "fichier mis à jour".

2️⃣ google.storage.object.delete
    🔹 Déclenché lorsqu’un objet est **supprimé** d’un bucket GCS.
    🔹 Attention : ne se déclenche pas si un objet est remplacé (c'est `finalize`).

3️⃣ google.storage.object.archive
    🔹 Déclenché lorsqu’un objet est **archivé** dans un bucket GCS avec versioning activé.
    🔹 Cela arrive lorsqu’une nouvelle version d’un objet est écrite et que l’ancienne est conservée.

4️⃣ google.storage.object.metadataUpdate
    🔹 Déclenché lorsqu’un objet voit ses **métadonnées modifiées**, sans modification du contenu.
    🔹 Par exemple, un changement de type MIME ou de métadonnée personnalisée.

📝 Exemple d’usage dans le déploiement :
  --trigger-event=google.storage.object.finalize
  --trigger-event=google.storage.object.delete
  --trigger-event=google.storage.object.archive
  --trigger-event=google.storage.object.metadataUpdate"""
