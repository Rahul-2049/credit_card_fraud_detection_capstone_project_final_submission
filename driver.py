import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from rules.rules import *
import json

# Initialize Spark session    
spark = SparkSession.builder.appName("CreditCardFraud").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read stream from Kafka
credit_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "transactions-topic-verified") \
    .load()

# Clean JSON values
def clean_json(json_str):
    return json_str.replace('\n', '').replace('\r', '').strip()

clean_json_udf = udf(clean_json, StringType())

# Validate JSON
def validate_json(json_str):
    try:
        json.loads(json_str)
        return True
    except json.JSONDecodeError:
        return False

validate_json_udf = udf(validate_json, BooleanType())

# Define schema for transaction
dataSchema = StructType([
    StructField("card_id", LongType(), True),
    StructField("member_id", LongType(), True),
    StructField("amount", DoubleType(), True),
    StructField("pos_id", LongType(), True),
    StructField("postcode", IntegerType(), True),
    StructField("transaction_dt", StringType(), True)
])

# Clean and validate JSON
cleanedDF = credit_data.selectExpr("cast(key as string)", "cast(value as string)") \
    .withColumn("cleaned_value", clean_json_udf(col("value")))

validatedDF = cleanedDF.withColumn("is_valid_json", validate_json_udf(col("cleaned_value")))

# Parse JSON with additional cleaning
parsed = validatedDF \
    .filter(col("is_valid_json")) \
    .withColumn("cleaned_value", regexp_replace(col("cleaned_value"), "\\\\", "")) \
    .withColumn("cleaned_value", regexp_replace(col("cleaned_value"), "^\"|\"$", "")) \
    .select(from_json(col("cleaned_value"), dataSchema).alias("credit_data"))

# Extract parsed fields and filter nulls
credit_data_stream = parsed.select("credit_data.*") \
    .filter(
        col("card_id").isNotNull() &
        col("member_id").isNotNull() &
        col("amount").isNotNull() &
        col("postcode").isNotNull() &
        col("pos_id").isNotNull() &
        col("transaction_dt").isNotNull()
    )

# Define UDF which verifies all the rules for each transaction and updates the lookup and master tables
verify_all_rules = udf(verify_rules_status, StringType())

Final_data = credit_data_stream \
    .withColumn('status', verify_all_rules(credit_data_stream['card_id'],
                                           credit_data_stream['member_id'],
                                           credit_data_stream['amount'],
                                           credit_data_stream['pos_id'],
                                           credit_data_stream['postcode'],
                                           credit_data_stream['transaction_dt']))

# Write output to console as well
output_data = Final_data \
    .select("card_id", "member_id", "amount", "pos_id", "postcode", "transaction_dt") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Indicating Spark to await termination
output_data.awaitTermination()
