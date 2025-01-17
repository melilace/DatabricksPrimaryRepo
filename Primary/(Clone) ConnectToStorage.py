# Databricks notebook source
dbutils.fs.mount(  
  source = "wasbs://containerdbx1@storageblobdbx.blob.core.windows.net/",  
  mount_point = "/mnt/mntpointblobstoragedbx1",  
  extra_configs = {"fs.azure.account.key.storageblobdbx.blob.core.windows.net":"yYeZTtei0jZS7h+6jZB1bqdveK91U6qGf6O22tSIGaFYAfXuqLW3zdVceakGUKDK6OcID9S4uV3h+AStdidnuA=="})
##Create Mount point


# COMMAND ----------

source = "wasbs://containerdbx1@storageblobdbx.blob.core.windows.net/",  
 


# COMMAND ----------

dbutils.fs.ls("/mnt/mntpointblobstoragedbx1")
##Verify Existence of mount point
