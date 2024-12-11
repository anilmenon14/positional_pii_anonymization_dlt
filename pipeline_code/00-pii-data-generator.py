# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fake data generator for `customers`
# MAGIC
# MAGIC Run this notebook to create new data. It's added in the pipeline to make sure data exists when we run it.
# MAGIC
# MAGIC You can also run it in the background to periodically add data.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&org_id=1444828305810485&notebook=%2F_resources%2F00-Data_CDC_Generator&demo_name=dlt-cdc&event=VIEW&path=%2F_dbdemos%2Fdata-engineering%2Fdlt-cdc%2F_resources%2F00-Data_CDC_Generator&version=1">

# COMMAND ----------
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
fake = Faker()
import random
import sys

# COMMAND ----------
# Define DBFS folder where data will land
try:
    folder = dbutils.widgets.get("ingestion_folder")
except Exception as e:
    print(f"An error occurred: {e}.\n It is likely the'ingestion_folder' parameter was not provided.")
    sys.exit(1)  # Exit with status 1 indicating an error

try:
    numRecords = int(dbutils.widgets.get("num_fake_records"))# Retrieve from parameters passed down from pipeline
except:
    numRecords = 100000

try:
    freeTextCols = int(dbutils.widgets.get("num_freetext_cols"))# Retrieve from parameters passed down from pipeline
except:
    numRecords = 2 # Default is set to 2

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
operations = OrderedDict([("APPEND", 0.5),("DELETE", 0.1),("UPDATE", 0.3),(None, 0.01)])
fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
fake_text_udf = F.udf(lambda: " ".join(fake.sentences(nb=random.randint(3, 10)))).asNondeterministic() # 'asNondeterministic' to allow for unique value across columns

# COMMAND ----------
df = spark.range(0, numRecords).repartition(100)
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("operation", fake_operation())

# Adding freetext columns based on input from user
for i in range(1, freeTextCols+1):
    df = df.withColumn(f"freetext_{i}", fake_text_udf())
df = df.withColumn("operation_date", fake_date())


df.repartition(100).write.format("json").mode("append").save(folder+"/customers")