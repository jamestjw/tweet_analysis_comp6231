CLUSTER_NAME=sentiment-analysis-cluster-$(date +%s)
REGION=us-east1

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'PIP_PACKAGES=google-cloud-storage spark-nlp==2.7.2' \
    --worker-machine-type n1-standard-8 \
    --num-workers 4 \
    --image-version 1.4-debian10 \
    --initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh \
    --optional-components=JUPYTER,ANACONDA \
    --enable-component-gateway \
    --project="comp-6231-356417" \
    --max-idle 20m \

gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME}\
    --region ${REGION}\
    --properties=spark.jars.packages=com.johnsnowlabs.nlp:spark-nlp_2.11:2.7.2\
    --driver-log-levels root=FATAL \
    --project="comp-6231-356417" \
    generate-sentiment-analysis.py
