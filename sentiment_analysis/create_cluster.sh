CLUSTER_NAME=sentiment-analysis-cluster-$(date +%s)
REGION=us-east1
NUM_WORKERS=5
DATE=2022-05-01

# Worker machine type: 8 cores and 45gb * 1024 memory
gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh \
    --metadata 'PIP_PACKAGES=google-cloud-storage spark-nlp' \
    --worker-machine-type n1-standard-8 \
    --master-boot-disk-size=128GB \
    --worker-boot-disk-size=128GB \
    --num-workers ${NUM_WORKERS} \
    --image-version 2.0 \
    --optional-components=JUPYTER \
    --enable-component-gateway \
    --project="comp-6231-356417" \
    --max-idle 20m \
    --properties spark:spark.serializer=org.apache.spark.serializer.KryoSerializer,spark:spark.driver.maxResultSize=0,spark:spark.kryoserializer.buffer.max=2000M,spark:spark.jars.packages=com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.2

gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME}\
    --region ${REGION}\
    --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --properties="spark.jars.packages=com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.2,spark.kryoserializer.buffer.max=2000M,spark.serializer=org.apache.spark.serializer.KryoSerializer,spark.driver.maxResultSize=0"\
    --driver-log-levels root=FATAL \
    --project="comp-6231-356417" \
    generate-sentiment-analysis.py -- ${DATE}

