from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sparknlp
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = sparknlp.start()

parser = argparse.ArgumentParser()
parser.add_argument("date", help="Date to run the data on")
args = parser.parse_args()

logger.info(f"Running job for date {args.date}")

trainDataset = (
    spark.read.format("bigquery")
    .option("table", "comp-6231-356417:Twitter.Tweets")
    .option("filter", f'DATE(created_at) = DATE("{args.date}")')
    .load()
)

logger.info(f"Processing {trainDataset.count()} rows.")

documentAssembler = DocumentAssembler().setInputCol("content").setOutputCol("document")

use = (
    UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")
    .setInputCols(["document"])
    .setOutputCol("sentence_embeddings")
)

emotion_classifier = (
    ClassifierDLModel.pretrained(name="classifierdl_use_emotion")
    .setInputCols(["sentence_embeddings"])
    .setOutputCol("emotion")
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
        emotion_classifier,
        sarcasm_classifier,
        cyberbullying_classifier,
    ]
)

logger.info("Pipeline setup successfully.")

empty_df = spark.createDataFrame([[""]]).toDF("Text")

logger.info("Starting transformation of data")

pipelineModel = nlpPipeline.fit(empty_df)
result = pipelineModel.transform(trainDataset)

result.select(
    F.col("tweet_id"),
    F.expr("emotion.result[0]").alias("emotion"),
    F.expr("sarcasm.result[0]").alias("sarcasm"),
    F.expr("cyberbullying.result[0]").alias("cyberbullying"),
    F.current_timestamp().alias("created_at"),
).write.format("bigquery").option(
    "temporaryGcsBucket", "tweet_analysis_temp_bucket"
).mode(
    "append"
).save(
    "Twitter.sentiment_analysis"
)
