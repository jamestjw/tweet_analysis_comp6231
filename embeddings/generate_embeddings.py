import time

from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

OUTPUT_DIR = "gs://tweet_analysis/embeddings/"

# Create a SparkSession under the name "embeddings". Viewable via the Spark UI
spark = SparkSession.builder.appName("embeddings twitter").getOrCreate()

# TODO: Read data from bigtable instead
data_file = "tweet_dump_2022-06-16.csv"
gs_uri = f"gs://tweet_analysis/{data_file}"
dataset = spark.read.option("header", True).csv(gs_uri).na.drop(subset=["Text"])

# We need to convert plain text to a Spark Document type
document_assembler = DocumentAssembler().setInputCol("Text").setOutputCol("document")

MODEL_NAME = "sent_small_bert_L2_256"

embeddingsSentence = (
    BertSentenceEmbeddings.pretrained(MODEL_NAME, "en")
    .setInputCols("document")
    .setOutputCol("sentence_bert_embeddings")
)

bert_pipeline = Pipeline(
    stages=[document_assembler, embeddingsSentence]
)

# Generate embeddings for dataframe
df_bert = bert_pipeline.fit(dataset).transform(dataset)
df_bert = df_bert.withColumn(
    "sentence_bert_embeddings", df_bert.sentence_embeddings[0].embeddings
)

output_gs_uri = f"{OUTPUT_DIR}/bert-embeddings-{MODEL_NAME}-{data_file}.parquet"
# Only save the tweet_id and the embeddings
df_bert.select(
    F.col("Tweet Id").alias("tweet_id"),
    F.col("sentence_bert_embeddings").alias("embeddings"),
).write.mode("overwrite").parquet(output_gs_uri)
