# tweet_analysis_comp6231

Scripts to run tweet analysis.

## Embeddings
NOTE: Ultimately this wasn't used as we included the production of embeddings in the pipelines of the other tasks

A Pyspark script is provided to produce sentence embeddings and save results in GCS (Cloud Storage). (TODO: Read input data from BigTable)

A Shell script is provided to create a Dataproc cluster and submit the job (the aforementioned Pyspark script) to it.

## Clustering
NOTE: This was ultimately abandonned as the performance was not good enough for this to be practical. An efficient implementation of the DBScan algorithm has to be written for this to actually work.

A Pyspark script that reads sentence embeddings from Google Cloud Storage (in a parquet format) and runs a DBScan clustering algorithm on them.

## Sentiment Analysis
A Pyspark script that reads tweets from Google BigQuery and runs the below pipeline to run sentiment analysis on them.
![sentiment-analysis-pipeline](http://www.plantuml.com/plantuml/png/VP31IWCn48Rl-nJ3dbgXho2rrbi5In4FjaWscMu7aacPcLZsxKsWQW_Y9H2-xvzX_gMQ4oifThExEIo4nfCjxg7VP1V0BeYcS9ISOBNjkn_ylaFxfkMyPx0C0y9PiaTL37ic1ZiXvFe8r1qZwW7MsPU4R0_LIKD4kPdu8ZaKZw2L8tlSSrKm1EAn9b-PFb7KnTqLFx7FeGq8-SAtUH-TAXt9EwFC1tYy_b7Mc-SQYJ33b1kRTuk8nxhyBqUTUAVfNyTF0yfGOfo8nr-pVnCoX5piNAmrqqwYTlU7OnSd9MsDkLe04CEAncr1cVfkYHnAYby0)

This Spark job produces 3 outputs:
- Emotion of the tweet (`sadness`, `joy`, `love`, `anger`, `fear`, or `surprise`)
- Sarcasm of the tweet (`normal` or `sarcasm`)
- Presence of cyberbullying in the tweet (`neutral`, `racism` or `sexism`)

The output of this Spark job is added to a BigQuery table
