#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession
import dbscan
from scipy.spatial.distance import euclidean
from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    IntegerType,
    DoubleType,
)
import seaborn as sns
from sklearn.datasets import make_blobs
import matplotlib.pyplot as plt

spark = (
    SparkSession.builder.appName("clusteringtest")
    #     .config("spark.jars.packages", "graphframes:graphframes-0.8.2-spark3.1-s_2.12")
    # This line may or may not be necessary
    .getOrCreate()
)


# Generate dummy data

centers = [[1, 1], [-1, -1], [1, -1], [2, 2], [-2, -2], [2, -2]]
X, labels_true = make_blobs(
    n_samples=1500, centers=centers, cluster_std=0.4, random_state=5
)
plt.subplots(figsize=(6, 6))
plt.plot(
    X[:, 0],
    X[:, 1],
    "o",
    markerfacecolor=[0, 0, 0, 1],
    markeredgecolor="k",
    markersize=3,
    scalex=1,
    scaley=1,
)
plt.title("Data Points")


# Put dummy data in Spark dataframe

# In[6]:


schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("value", ArrayType(DoubleType(), False), False),
    ]
)

# Convert list to RDD
rdd = spark.sparkContext.parallelize(zip(range(len(X)), X.tolist()))

# Create data frame
df = spark.createDataFrame(rdd, schema)


# In[7]:


res = dbscan.process(
    spark,
    df,
    epsilon=0.3,
    min_pts=10,
    dist=euclidean,
    dim=2,
    checkpoint_dir="./checkpoint",
    operations=None,
)


# In[8]:


# Point is the ID
# Component is the cluster ID
res.show(10)


# In[9]:


res.select("component").distinct().collect()  # Three clear clusters


# Merge original DF with the results of clustering

# In[10]:


cluster_df = res.toPandas()
data_df = df.toPandas()
final_df = data_df.merge(cluster_df, how="left", left_on="id", right_on="point")
final_df["x_coord"] = final_df["value"].map(lambda x: x[0])
final_df["y_coord"] = final_df["value"].map(lambda x: x[1])


# In[11]:


fig, ax = plt.subplots(figsize=(5, 5))
sns.scatterplot(
    x="x_coord", y="y_coord", hue="component", data=final_df, legend="full", ax=ax
)


# In[14]:


OUTPUT_DIR = "gs://tweet_analysis/clustering"
fig.savefig(f"{OUTPUT_DIR}/clustering-poc-output.png")
