# Databricks notebook source
df = spark.read.format("bigquery")\
    .option("parentProject","swordhealth-production-us")\
    .option("project","swordhealth-production-us")\
    .option("table","eligibility_prd.stg_eligibility_source")\
    .load()     

# COMMAND ----------

l=["cardinalhealth"]

# COMMAND ----------

l1=["wpshealthinsurance"]

# COMMAND ----------

l2=["danaher"]

# COMMAND ----------

l3= ["sana"]

# COMMAND ----------

l4= ["harborfreight"]

# COMMAND ----------

l5= ["meritain_aaacluballiance"]

# COMMAND ----------

l6= ["gravie"]

# COMMAND ----------

l5= ["pepsiCo"]

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%117%'"))

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/gravie/outbound/claims/med/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/gravie/outbound/claims/med/*.csv")
header = df.columns

# COMMAND ----------

import json

def assign_number(headerElements):
    output_dict = {}
    for index, headerElements in enumerate(headerElements, start=1):
        output_dict[index] = headerElements
    return output_dict

result = assign_number(header)
formated_result = json.dumps(result, indent = 2)
print(formated_result)

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/gravie/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/gravie/outbound/claims/rx/*.csv")
header = df.columns

# COMMAND ----------

import json

def assign_number(headerElements):
    output_dict = {}
    for index, headerElements in enumerate(headerElements, start=1):
        output_dict[index] = headerElements
    return output_dict

result = assign_number(header)
formated_result = json.dumps(result, indent = 2)
print(formated_result)

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/PepsiCo/outbound/claims/med/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/PepsiCo/outbound/claims/med/*.txt")
header = df.columns


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", True).option("delimiter", "|").option("inferSchema", True).load("gs://sword-sftp-production-us-private/PepsiCo/outbound/claims/med/*.txt")
header = df.columns

print(header)

# COMMAND ----------

import json

def assign_number(headerElements):
    output_dict = {}
    for index, headerElements in enumerate(headerElements, start=1):
        output_dict[index] = headerElements
    return output_dict

result = assign_number(header)
formated_result = json.dumps(result, indent = 2)
print(formated_result)

# COMMAND ----------

c=1
for i in header:
    print(i,c)
    c= c+1

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/PepsiCo/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/PepsiCo/outbound/claims/med/*.txt")
header = df.columns

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", True).option("delimiter", "|").option("inferSchema", True).load("gs://sword-sftp-production-us-private/PepsiCo/outbound/claims/rx/*.txt")
header = df.columns

print(header)

# COMMAND ----------

import json

def assign_number(headerElements):
    output_dict = {}
    for index, headerElements in enumerate(headerElements, start=1):
        output_dict[index] = headerElements
    return output_dict

result = assign_number(header)
formated_result = json.dumps(result, indent = 2)
print(formated_result)

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/med/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/med/*.txt")
header = df.columns

print(header)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", True).option("delimiter", "|").option("inferSchema", True).load("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/med/*.txt")
header = df.columns

print(header)

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", True).option("delimiter", "|").option("inferSchema", True).load("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/rx/*.txt")
header = df.columns

print(header)

# COMMAND ----------

import json

def assign_number(headerElements):
    output_dict = {}
    for index, headerElements in enumerate(headerElements, start=1):
        output_dict[index] = headerElements
    return output_dict

result = assign_number(header)
formated_result = json.dumps(result, indent = 2)
print(formated_result)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.format("csv").option("header", True).option("delimiter", "|").load("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/rx/*.txt")
header = df.columns

print(header)

# COMMAND ----------

import json

def assign_number(headerElements):
    output_dict = {}
    for index, headerElements in enumerate(headerElements, start=1):
        output_dict[index] = headerElements
    return output_dict

result = assign_number(header)
formated_result = json.dumps(result, indent = 2)
print(formated_result)


# COMMAND ----------

c=1
for i in header:
    print(i,c)
    c= c+1

# COMMAND ----------

for i in l4:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'"))

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%107%'"))

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/anthem/anthem_harborfreight_aso/outbound/claims/med")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/anthem/anthem_harborfreight_aso/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/anthem/anthem_harborfreight_aso/outbound/claims/med/*.csv")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

df. schema

# COMMAND ----------

c=1
for i in header:
    print(i,c)
    c= c+1

# COMMAND ----------

for i in l3:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%106%'"))

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/sana_benefits/outbound/claims/med/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/sana_benefits/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/sana_benefits/outbound/claims/med/*.csv")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

c=1
for i in header:
    print(i,c)
    c= c+1

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/sana_benefits/outbound/claims/rx/*.csv")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

c=1
for i in header:
    print(i,c)
    c= c+1

# COMMAND ----------

for i in l2:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

for i in l:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%64%'")) 

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%229%'")) 

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%110%'")) 

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/eligibility/files/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/claims/med/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/eligibility/files/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/claims/med/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/alight_cardinalhealth/outbound/eligibility/files/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/alight_cardinalhealth/outbound/claims/med/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/alight_cardinalhealth/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/alight_cardinalhealth/outbound/eligibility/files/*.csv")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%64%'")) 

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/umr/umr_globus/outbound/eligibility/files/")

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%245%'")) 

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/umr/umr_globus/outbound/eligibility/files/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/umr/umr_globus/outbound/eligibility/files/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/gravie/outbound/eligibility/files/")

# COMMAND ----------

file_list=dbutils.fs.ls("gs://sword-sftp-production-us-private/wpshealthinsurance/")
file_paths = [file.path for file in file_list if file.name.endswith(".txt")]

file_paths

# COMMAND ----------

file_list=dbutils.fs.ls("gs://sword-sftp-production-us-private/meritain_aaacluballiance/")
file_paths = [file.path for file in file_list if file.name.endswith(".txt")]

file_paths

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/eligibility/files/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/claims/med/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/eligibility/files/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/claims/med/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/wpshealthinsurance/outbound/claims/rx/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/eligibility/files/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/eligibility/files/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/med/")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/med/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/rx/")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/meritain_aaacluballiance/outbound/claims/rx/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)
