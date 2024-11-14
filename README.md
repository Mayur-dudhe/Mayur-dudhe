import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize GlueContext and SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define your Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'REDSHIFT_URL', 'REDSHIFT_DB', 'REDSHIFT_TABLE', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD'])

# Extract: Read CSV data from S3
s3_input_path = args['S3_INPUT_PATH']  # S3 path where your CSV files are located
df = spark.read.format("csv").option("header", "true").load(s3_input_path)

# Transform: Apply transformations (Example: remove rows with null values)
transformed_df = df.dropna()

# Example Transformation: Rename columns if necessary
transformed_df = transformed_df.withColumnRenamed("old_column_name", "new_column_name")

# Load: Write transformed data to Amazon Redshift
redshift_url = args['REDSHIFT_URL']
redshift_db = args['REDSHIFT_DB']
redshift_table = args['REDSHIFT_TABLE']
redshift_user = args['REDSHIFT_USER']
redshift_password = args['REDSHIFT_PASSWORD']

# Define Redshift connection options
redshift_options = {
    "url": f"jdbc:redshift://{redshift_url}:5439/{redshift_db}",
    "dbtable": redshift_table,
    "user": redshift_user,
    "password": redshift_password,
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Write data to Redshift 
transformed_df.write.format("jdbc").options(**redshift_options).mode("append").save()

print("ETL job completed successfully.")
