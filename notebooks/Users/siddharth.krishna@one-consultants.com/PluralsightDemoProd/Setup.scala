// Databricks notebook source
// MAGIC %md ###Section 1: Mount Azure Data Lake Store

// COMMAND ----------

dbutils.fs.unmount("/mnt/datalake")

val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "469455e9-dd7f-4b80-9e0d-c1afaa86a41b",
  "dfs.adls.oauth2.credential" -> "KKrYgQGjQri8J=1@UtU/09AbAZ:n-B@s",
  "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/8a7bb8e8-a0ba-4b1a-95cb-5169eb7ae0f0/oauth2/token")

// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://onedatalakeadls.azuredatalakestore.net/DatabricksDemo/Files",
  mountPoint = "/mnt/datalake",
  extraConfigs = configs)


// COMMAND ----------

// MAGIC %md ###Section 2: Mount Azure Blob Storage

// COMMAND ----------

dbutils.fs.unmount("/mnt/storage")

val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "469455e9-dd7f-4b80-9e0d-c1afaa86a41b",
  "dfs.adls.oauth2.credential" -> "KKrYgQGjQri8J=1@UtU/09AbAZ:n-B@s",
  "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/8a7bb8e8-a0ba-4b1a-95cb-5169eb7ae0f0/oauth2/token")

// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://onedatalakeadls.azuredatalakestore.net/DatabricksDemo/Files",
  mountPoint = "/mnt/storage",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %fs ls "/mnt/datalake"