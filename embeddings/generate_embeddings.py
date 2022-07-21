import time

import sparknlp

from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession

# Create a SparkSession under the name "embeddings". Viewable via the Spark UI
spark = SparkSession.builder.appName("embeddings twitter").getOrCreate()

# TODO: Read data from bigtable instead
data_file = "tweet_dump_2022-06-16.csv"
gs_uri = f"gs://tweet_analysis/{data_file}"
dataset = spark.read \
               .option("header", True) \
               .csv(gs_uri) \
               .na.drop(subset=["Text"])

# We need to convert plain text to a Spark Document type
document_assembler = DocumentAssembler() \
    .setInputCol("Text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

bert_embeddings = BertEmbeddings().pretrained(name='small_bert_L4_256', lang='en') \
    .setInputCols(["document",'token'])\
    .setOutputCol("embeddings")

embeddingsSentence = SentenceEmbeddings() \
    .setInputCols(["document", "embeddings"]) \
    .setOutputCol("sentence_embeddings") \
    .setPoolingStrategy("AVERAGE")

bert_pipeline = Pipeline(stages=[
    document_assembler,
    tokenizer,
    bert_embeddings,
    embeddingsSentence
])

## Generate embeddings for dataframe
df_bert = bert_pipeline.fit(dataset).transform(dataset)
df_bert = df_bert.drop("document", "token", "embeddings")

output_gs_uri = f"gs://tweet_analysis/bert-embeddings-{data_file}-{int(time.time())}.parquet"
df_bert.withColumnRenamed("Tweet Id", "tweet_id")\
      .write.save(output_gs_uri)
