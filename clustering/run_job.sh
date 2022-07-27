CLUSTER_NAME=clustering-cluster-$(date +%s)
REGION=us-east1
PIP_PACKAGES=$(tr '\n' ' ' < requirements.txt)

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --image-version=2.0 \
    --metadata "PIP_PACKAGES=${PIP_PACKAGES}" \
    --worker-machine-type n1-standard-8 \
    --num-workers 2 \
    --image-version 1.4-debian10 \
    --initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh \
    --optional-components=JUPYTER,ANACONDA \
    --enable-component-gateway \
    --project="comp-6231-356417" \
    --max-idle 10m \

gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME}\
    --region ${REGION}\
    --properties=spark.jars.packages=com.johnsnowlabs.nlp:spark-nlp_2.11:2.7.2\
    --driver-log-levels root=FATAL \
    --project="comp-6231-356417" \
    dummy-clustering.py
