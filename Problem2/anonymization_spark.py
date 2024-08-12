from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Anonymize CSV Data") \
    .getOrCreate()

# Load the CSV file
df = spark.read.csv('./large_dataset_spark/*', header=True, inferSchema=True)

# Anonymize specific columns
df_anonymized = df.withColumn("first_name", sha2("first_name", 256)) \
                  .withColumn("last_name", sha2("last_name", 256)) \
                  .withColumn("address", sha2("address", 256))

# Save the anonymized data back to CSV
df_anonymized.write.csv('anonymized_large_dataset.csv', header=True)

# Stop the Spark session
spark.stop()
