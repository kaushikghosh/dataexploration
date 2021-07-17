# Databricks notebook source
# DBTITLE 1,Set storage configuration and explore data
# We will first set the account name and storage key details first and get the description of columns in the sample datset
#Set the Hadoop Azure Configuration first
spark.conf.set("fs.azure.account.key.storage_name.blob.core.windows.net", "storage_key")
file_path = "wasbs://container_name@storage_name.blob.core.windows.net/wikipedia-2016-06-27-sampled.json"
df = spark.read.json(file_path)
df.printSchema()
df.show()

# COMMAND ----------

# DBTITLE 1,Let's group the dataset by the page title 
pageDf = df.groupBy(['page']).count()
pageDf.display()

# COMMAND ----------

# DBTITLE 1,Now let us look at the edits by country
pageEditsDf = df.groupBy(['page', 'countryName']).count()
#In the above query there may be rows which are missing the countryName col values so we will drop rows with null values 
pageEditsDf.na.drop(subset=["countryName"]).show(truncate=False)
#instead of droping selective columns with null values we can also drop all rows with null values on all the columns pageEditsDf.na.drop("all").show(false)
# a variation of the above function using dropna() function will be : pageEditsDf.dropna().show(truncate=False)
pageEditsDf.na.drop(subset=["countryName"]).display()

# COMMAND ----------

# DBTITLE 1,Generalized lookup
import pyspark.sql.functions as func
#Lets do a more generalized lookup based on the channels from which the pages got added
#channelDf = df.groupBy('channel', 'page').agg(func.sum('added').alias('total_added')).orderBy('total_added', ascending=False).show()
#if we want to build the above query between 2 date ranges then it will go as below
channelDf = df.where((df.timestamp > '2016-06-27') & (df.timestamp < '2016-06-28')).groupBy('channel', 'page').agg(func.sum('added').alias('total_added')).orderBy('total_added', ascending=False).display()
