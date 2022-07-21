# tweet_analysis_comp6231

Scripts to run tweet analysis.

## Embeddings
A Pyspark script is provided to produce sentence embeddings and save results in GCS (Cloud Storage). (TODO: Read input data from BigTable)

A Shell script is provided to create a Dataproc cluster and submit the job (the aforementioned Pyspark script) to it.
