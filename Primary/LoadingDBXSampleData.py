# Databricks notebook source


# COMMAND ----------

df = spark.read.format("parquet").load("/databricks-datasets/samples/lending_club/parquet/")  
display(df)  
df.write.format("parquet").save("/mnt/your-storage-location/dat3a.parquet")  

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/airlines/

# COMMAND ----------

# Load all CSV files from the specified directory into a DataFrame
# Assumes first row is header
# Infers the data types; set to false if schema is known for better performance
flights_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/databricks-datasets/asa/airlines/*.csv")

# Display the DataFrame to see the data
display(flights_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog default;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# Display DataFrame
display(flights_df)

# Create a permanent table without specifying the catalog namespace
flights_df.write.format("parquet").saveAsTable("flights_table49894")

# Execute SQL query
result_df = spark.sql("""
SELECT Origin, Dest, COUNT(*) as TotalFlights
FROM flights_table
GROUP BY Origin, Dest
ORDER BY TotalFlights DESC
LIMIT 10
""")

# Display SQL query results
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `default`; select * from `default`.`flights_table3` limit 100;

# COMMAND ----------

# Assuming the DataFrame to be saved is named `flights_df`
flights_df.write.format("parquet").saveAsTable("mlworkspaceprimary.default.flights_table")

# COMMAND ----------

# Display DataFrame  
display(flights_df)  
  
# Create a permanent table  
flights_df.write.format("parquet").saveAsTable("mlworkspaceprimary.default.flights_table45687")  
  
# Execute SQL query  
result_df = spark.sql("""  
SELECT Origin, Dest, COUNT(*) as TotalFlights  
FROM flights_table  
GROUP BY Origin, Dest  
ORDER BY TotalFlights DESC  
LIMIT 10  
""")  
  
# Display SQL query results  
display(result_df)

# COMMAND ----------

# Display DataFrame  
display(flights_df)  
  
# Create a permanent table without specifying the catalog namespace
flights_df.write.format("parquet").saveAsTable("default.flights_table45687")  
  
# Execute SQL query  
result_df = spark.sql("""  
SELECT Origin, Dest, COUNT(*) as TotalFlights  
FROM flights_table45687  
GROUP BY Origin, Dest  
ORDER BY TotalFlights DESC  
LIMIT 10  
""")  
  
# Display SQL query results  
display(result_df)

# COMMAND ----------

# Save as a permanent table  
flights_df.write.format("csv").option("header", "true").saveAsTable("flights_data_table")  

# COMMAND ----------

# Load all CSV files into DataFrame  
flights_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/databricks-datasets/asa/airlines/*.csv")  
  
# Display DataFrame  
display(flights_df)  
  
# Create a temporary view  
flights_df.createOrReplaceTempView("flights_view")  
  
# Execute SQL query  
result_df = spark.sql("""  
SELECT Origin, Dest, COUNT(*) as TotalFlights  
FROM flights_view  
GROUP BY Origin, Dest  
ORDER BY TotalFlights DESC  
LIMIT 10  
""")  
  
# Display SQL query results  
display(result_df)  
  
# Save as a permanent table  
flights_df.write.format("csv").option("header", "true").saveAsTable("flights_data_table2")

# COMMAND ----------

# Display the schema for df
df.printSchema()

# Display the schema for flights_df
flights_df.printSchema()

# Display the schema for result_df
result_df.printSchema()

# COMMAND ----------

df = spark.read.format("parquet").load("/databricks-datasets/samples/lending_club/parquet/")
df.write.format("delta").saveAsTable("default.lending_club_data2")
