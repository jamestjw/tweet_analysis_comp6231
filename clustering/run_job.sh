CLUSTER_NAME=clustering-cluster-$(date +%s)
REGION=us-east1
PIP_PACKAGES=$(tr '\n' ' ' < requirements.txt)
PROJECT_NAME=comp-6231-356417

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --image-version=2.0 \
    --metadata "PIP_PACKAGES=google-cloud-storage ${PIP_PACKAGES}" \
    --worker-machine-type n1-standard-8 \
    --num-workers 2 \
    --initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh \
    --enable-component-gateway \
    --project="${PROJECT_NAME}" \
    --max-idle 20m \

gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME}\
    --region ${REGION}\
    --properties=spark.jars.packages=graphframes:graphframes:0.8.2-spark3.1-s_2.12\
    --driver-log-levels root=FATAL \
    --project="${PROJECT_NAME}" \
    dummy-clustering.py
