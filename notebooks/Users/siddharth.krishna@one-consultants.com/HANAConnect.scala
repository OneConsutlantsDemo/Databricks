// Databricks notebook source
// MAGIC %scala
// MAGIC 
// MAGIC import java.util.Properties
// MAGIC 
// MAGIC //Set connection parameters 
// MAGIC val jdbcHostname = "hana-02.one-consultants.net"
// MAGIC val jdbcPort = "30041"
// MAGIC val jdbcDB = "HDB"
// MAGIC val jdbcUser = "SIDDHARTH"
// MAGIC val jdbcPassword = dbutils.secrets.get(scope="sid1805", key="sid1805")
// MAGIC val driverClass = "com.sap.db.jdbc.Driver"
// MAGIC val jdbcUrl = s"jdbc:sap://${jdbcHostname}:${jdbcPort}"
// MAGIC 
// MAGIC //Check availability of the JDBC library to access SAP HANA
// MAGIC Class.forName(driverClass)
// MAGIC 
// MAGIC //Set connection properties
// MAGIC val connectionProperties = new Properties()
// MAGIC connectionProperties.put("user", s"${jdbcUser}")
// MAGIC connectionProperties.put("password", s"${jdbcPassword}")
// MAGIC connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

val storage_account_access_key = dbutils.secrets.get(scope="sid1805", key="sid1805")


// COMMAND ----------

val TAXI_WITHOUTPAYMENTDATA = spark.read.jdbc(jdbcUrl, "ONE.TAXI_WITHOUTPAYMENTDATA", connectionProperties)


// COMMAND ----------

TAXI_WITHOUTPAYMENTDATA.write.format("delta").mode("overwrite").save("/mnt/datalake/Demo")

// COMMAND ----------

// MAGIC %sql 
// MAGIC DROP TABLE TAXI_WITHOUTPAYMENTDATA

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE TAXI_WITHOUTPAYMENTDATA
// MAGIC USING DELTA
// MAGIC LOCATION '/mnt/datalake/Demo'

// COMMAND ----------

Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
//val jdbcUsername = dbutils.secrets.get(scope = "jdbc", key = "username")
//val jdbcPassword = dbutils.secrets.get(scope = "jdbc", key = "password")

val jdbcUsername1 = "one_admin"
val jdbcPassword1 = dbutils.secrets.get(scope="sid1805", key="sidsql")

val jdbcHostname1 = "one-dw-01.database.windows.net"
val jdbcPort1 = 1433
val jdbcDatabase1 = "one-db-01"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl1 = s"jdbc:sqlserver://${jdbcHostname1}:${jdbcPort1};database=${jdbcDatabase1}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties1 = new Properties()

connectionProperties1.put("user", s"${jdbcUsername1}")
connectionProperties1.put("password", s"${jdbcPassword1}")

// COMMAND ----------

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties1.setProperty("Driver", driverClass)

// COMMAND ----------

val taxi_payment_lookup = spark.read.jdbc(jdbcUrl1, "[SHELL_POC].[TAXI_PAYMENT_TYPE]", connectionProperties1)

// COMMAND ----------

var taxi_payment_lookup1  = taxi_payment_lookup.withColumnRenamed("Payment_type","Payment_type_Desc")

// COMMAND ----------

taxi_payment_lookup1.write.mode("overwrite").saveAsTable("TAXI_PAYMENT_TYPE1")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE TAXI_WITHPAYMENTDATA 

// COMMAND ----------

// MAGIC %sql
// MAGIC create table TAXI_WITHPAYMENTDATA AS (
// MAGIC select * from TAXI_WITHOUTPAYMENTDATA a INNER JOIN TAXI_PAYMENT_TYPE1 b ON a.payment_type = b.Code)

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.table("TAXI_WITHPAYMENTDATA")
// MAGIC      .write
// MAGIC      .mode(SaveMode.Overwrite)
// MAGIC      .jdbc(jdbcUrl, "ONE.TAXI_WITHPAYMENTDATA", connectionProperties)