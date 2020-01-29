// Databricks notebook source
//dbutils.fs.unmount("/mnt/deltalake")

val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "469455e9-dd7f-4b80-9e0d-c1afaa86a41b",
  "dfs.adls.oauth2.credential" -> "KKrYgQGjQri8J=1@UtU/09AbAZ:n-B@s",
  "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/8a7bb8e8-a0ba-4b1a-95cb-5169eb7ae0f0/oauth2/token")

// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://onedatalakeadls.azuredatalakestore.net/DatabricksDemo",
  mountPoint = "/mnt/deltalake",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %fs ls /mnt/deltalake

// COMMAND ----------

// MAGIC %fs head "/mnt/deltalake/2018_Yellow_Taxi_Trip_Data_Full.csv"

// COMMAND ----------

var deltaframe = spark  
.read 
.option("header","true")
.csv ("/mnt/deltalake/2018_Yellow_Taxi_Trip_Data_Full.csv")

var deltaoptframe = spark  
.read 
.option("header","true")
.csv ("/mnt/deltalake/2018_Yellow_Taxi_Trip_Data_Full.csv")

var parquetframe = spark  
.read 
.option("header","true")
.csv ("/mnt/deltalake/2018_Yellow_Taxi_Trip_Data_Full.csv")





// COMMAND ----------

display(deltaframe)

// COMMAND ----------

deltaframe.write.format("Delta").mode(SaveMode.Overwrite).option("path", "/mnt/deltalake/Delta/taxi.parquet").saveAsTable("deltaframeTable")





// COMMAND ----------

parquetframe.write.format("Parquet").mode(SaveMode.Overwrite).option("path", "/mnt/deltalake/Parquet/taxi.parquet").saveAsTable("parquetframeTable")



// COMMAND ----------

deltaoptframe.write.format("Delta").mode(SaveMode.Overwrite).option("path", "/mnt/deltalake/DeltaOptimize/taxi.parquet").saveAsTable("deltaoptframeTable")



// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*),VendorID from parquetframeTable group by VendorID

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC select count(*),VendorID from deltaframeTable group by VendorID

// COMMAND ----------

// MAGIC %sql
// MAGIC optimize deltaoptframeTable

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*),VendorID from deltaoptframe group by VendorID

// COMMAND ----------

// MAGIC %sql
// MAGIC cache deltaoptframe

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC select count(*),VendorID from deltaoptframeTable group by VendorID

// COMMAND ----------

optimize deltaoptframeTable zorder by VendorID

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC select count(*),VendorID from deltaoptframeTable group by VendorID

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from deltaoptframeTable where VendorID = 4

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY deltaoptframeTable

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*),VendorID from deltaoptframeTable group by VendorID

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*),VendorID from deltaoptframeTable VERSION AS OF 0 group by VendorID

// COMMAND ----------

