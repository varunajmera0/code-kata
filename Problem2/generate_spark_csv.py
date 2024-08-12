from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DateType
import random
import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate Large CSV") \
    .getOrCreate()

# Define UDFs to generate fake data
def generate_first_name():
    return random.choice(["John", "Jane", "Alice", "Bob", "Charlie", "Dave", "Eva"])

def generate_last_name():
    return random.choice(["Doe", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"])

def generate_address():
    streets = ["Main St", "2nd St", "3rd St", "4th St", "Park Ave", "5th Ave", "6th Ave"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio"]
    return f"{random.randint(100, 999)} {random.choice(streets)}, {random.choice(cities)}, {random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX'])}, {random.randint(10000, 99999)}"

def generate_date_of_birth():
    start_date = datetime.date(1950, 1, 1)
    end_date = datetime.date(2000, 1, 1)
    return start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))

# Register UDFs
first_name_udf = udf(generate_first_name, StringType())
last_name_udf = udf(generate_last_name, StringType())
address_udf = udf(generate_address, StringType())
date_of_birth_udf = udf(generate_date_of_birth, DateType())

# Create a DataFrame with a large number of rows
num_rows = 20000000
df = spark.range(num_rows) \
    .withColumn("first_name", first_name_udf()) \
    .withColumn("last_name", last_name_udf()) \
    .withColumn("address", address_udf()) \
    .withColumn("date_of_birth", date_of_birth_udf())

# Write the DataFrame to a CSV file
df.select("first_name", "last_name", "address", "date_of_birth").coalesce(1).write.csv("large_dataset_spark", header=True)

# Stop the Spark session
spark.stop()
