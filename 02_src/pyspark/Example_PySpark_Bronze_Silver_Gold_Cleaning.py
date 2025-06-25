from pyspark.sql import SparkSession

# Initialize Spark session
spark =SparkSession.builder.appName("MedallionArchitectureExample").getOrCreate()

# Bronze Layer: Ingest raw data
raw_data_path = "/path/to/raw/data"
bronze_df = spark.read.format("csv").option("header","true").load(raw_data_path)
bronze_df.write.format("parquet").mode("overwrite").save("/path/to/bronze_layer")

# Silver Layer: Clean and transform data
bronze_df = spark.read.format("parquet").load("/path/to/bronze_layer")
silver_df = bronze_df.dropDuplicates().filter(bronze_df["value"].isNotNull())
silver_df.write.format("parquet").mode("overwrite").save("/path/to/silver_layer")

# Gold Layer: Aggregate and optimize data
silver_df = spark.read.format("parquet").load("/path/to/silver_layer")
gold_df = silver_df.groupBy("category").agg({"value":"sum"}).withColumnRenamed("sum(value)", "total_value")
gold_df.write.format("parquet").mode("overwrite").save("/path/to/gold_layer")