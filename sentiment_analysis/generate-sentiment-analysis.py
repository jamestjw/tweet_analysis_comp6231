from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sparknlp

spark = sparknlp.start()

data_filename_stem = "tweet_dump_2022-06-16"
input_file_dir = f"gs://tweet_analysis/{data_filename_stem}.csv"

trainDataset = (
    spark.read.option("header", True).csv(input_file_dir).na.drop(subset=["Text"])
)

documentAssembler = DocumentAssembler().setInputCol("Text").setOutputCol("document")

use = (
    UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")
    .setInputCols(["document"])
    .setOutputCol("sentence_embeddings")
)

sentimentdl = (
    ClassifierDLModel.pretrained(name="classifierdl_use_emotion")
    .setInputCols(["sentence_embeddings"])
    .setOutputCol("sentiment")
)

sarcasm_classifier = (
    ClassifierDLModel.pretrained(name="classifierdl_use_sarcasm")
    .setInputCols(["sentence_embeddings"])
    .setOutputCol("sarcasm")
)

cyberbullying_classifier = (
    ClassifierDLModel.pretrained("classifierdl_use_cyberbullying", "en")
    .setInputCols(["document", "sentence_embeddings"])
    .setOutputCol("cyberbullying")
)

nlpPipeline = Pipeline(
    stages=[
        documentAssembler,
        use,
        sentimentdl,
        sarcasm_classifier,
        cyberbullying_classifier,
    ]
)

empty_df = spark.createDataFrame([[""]]).toDF("Text")

pipelineModel = nlpPipeline.fit(empty_df)
result = pipelineModel.transform(trainDataset)

result.select(
    F.col("Tweet Id").alias("tweet_id"),
    F.expr("sentiment.result[0]").alias("sentiment"),
    F.expr("sarcasm.result[0]").alias("sarcasm"),
    F.expr("cyberbullying.result[0]").alias("cyberbullying"),
).write.format("bigquery") \
    .option("temporaryGcsBucket","tweet_analysis_temp_bucket") \
    .mode("append") \
    .save("Twitter.sentiment_analysis")
