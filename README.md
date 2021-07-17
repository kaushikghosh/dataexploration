# This is a part of series of my post on ways to explore data using tools normally used in Big data world for data transformations
In this tutorial we will first go over the wikipedia sample data provided by druid and explore the data using spark (pyspark). In the next part of the series we will look at validating the same results using the exmple wikipedia datasets and tally the same from druid query browser.
## Pre-requisites
We will be using Azure Blob storage as our object store which is complaint with Hadoop file storage. We will also be using the databricks community edition. You can sign up for the same through https://community.cloud.databricks.
## Settign up spark context to access the file on Azure Blob storage
When you first go on the community cloud version of databricks and create a python notebook. It will not be attached to any cluster. But as soon as you write any code and start to execute a cell, it will ask you you to start the cluster. From the left hand side menu where you see compute, select your cluster and go to libraries and install the maven jar file for hadoop-azure. Whatever is the latest version as its normally backward compatible. The following lines shows you how to access the file once you upload to the azure blob storage usign the storage_name, container_name, storage_key and the filename.

spark.conf.set("fs.azure.account.key.storage_name.blob.core.windows.net", "storage_key")
file_path = "wasbs://container_name@storage_name.blob.core.windows.net/wikipedia-2016-06-27-sampled.json"
df = spark.read.json(file_path)
df.printSchema()
df.show()

Once you execute the above steps you should be able to see the dataframe which will show you the schema of the sample dataset. Please follow along the notebook provided called wikipedia.py where there are enough comments and shows how to do most common operations like group by and aggregation. More to come, have fun.
