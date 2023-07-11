# Databricks notebook source


# COMMAND ----------

df = spark.read.format("bigquery")\
    .option("parentProject","swordhealth-production-us")\
    .option("project","swordhealth-production-us")\
    .option("table","eligibility_prd.stg_eligibility_source")\
    .load()        

# COMMAND ----------

list = ["humana_aso","bacorporation_ps486","uhc_nfp","ascension_abs",
"bcbstx_consolidatedcommunications",
"meritain_aaacluballiance",
"sana_benefits",
"concordia",
"aetna_arjo",
"aetna_bcg",
"Portico",
"uhc_pepsico",
"dhl"]

# COMMAND ----------

list1=["cottagehealth",
"cigna_overstock","PepsiCo",
"gravie",
"uhc_schneider",
"bcbstx_wood",
"uhc_cort",
"moda_health","allegiance_houstonmethodist",
"umr_flsmidth",
"footlocker",
"uhc_cardinalhealth",
"uhc_fmcna","meritain_simplisafe","bcbstx_nov",
"ewif","danaher","portico","evernorth_pepsico","wpshealthinsurance","foundationhealthconsultants_mimedx"]

# COMMAND ----------

list2=["uhc_uhs","meritain_firstamerican","anthem_harborfreight_aso","adp_arjo",
"adp_motrex","aetna_boomi","aetna_cohnreznick","aetna_dominos","aetna_pearson","aetna_phillips66","aetna_sap",
"aetna_uhs",
"alight_cardinalhealth",
"alight_govcio",
"alight_phillips66",
"alight_ruppert_landscape",
"alight_samsung_ssi",
"alight_ttec",
"allegiance_sibanye_stillwater",
"alliant_universityofphoenix",
"anthem"]

# COMMAND ----------

list3=["anthem_advantagesolutions",
"anthem_harborfreight",
"arc_best",
"avmed",
"axa_xl",
"battletest",
"bbt_sheet_metal",
"bcbsfl_ryam",
"bcbsnc_carolinadealerships",
"bcbsnd",
"bcbssc_sefl",
"bcbstn_uhs",
"bcbstx_phillips66",
"benefitfocus_sap",
"bridge_health",
"bswift_toyodagosei",
"bswift_xpo"]

# COMMAND ----------

list0 = ["humana_aso","bacorporation_ps486","uhc_nfp","ascension_abs",
"bcbstx_consolidatedcommunications",
"meritain_aaacluballiance",
"sana_benefits",
"concordia",
"aetna_arjo",
"aetna_bcg",
"Portico",
"uhc_pepsico",
"dhl",
"cottagehealth",
"cigna_overstock","PepsiCo",
"gravie",
"uhc_schneider",
"bcbstx_wood",
"uhc_cort",
"moda_health","allegiance_houstonmethodist",
"umr_flsmidth",
"footlocker",
"uhc_cardinalhealth",
"uhc_fmcna","meritain_simplisafe","bcbstx_nov",
"ewif","danaher","portico","evernorth_pepsico","wpshealthinsurance","foundationhealthconsultants_mimedx",
"uhc_uhs","meritain_firstamerican","anthem_harborfreight_aso","adp_arjo",
"adp_motrex","aetna_boomi","aetna_cohnreznick","aetna_dominos","aetna_pearson","aetna_phillips66","aetna_sap",
"aetna_uhs",
"alight_cardinalhealth",
"alight_govcio",
"alight_phillips66",
"alight_ruppert_landscape",
"alight_samsung_ssi",
"alight_ttec",
"allegiance_sibanye_stillwater",
"alliant_universityofphoenix",
"anthem",
"anthem_advantagesolutions",
"anthem_harborfreight",
"arc_best",
"avmed",
"axa_xl",
"battletest",
"bbt_sheet_metal",
"bcbsfl_ryam",
"bcbsnc_carolinadealerships",
"bcbsnd",
"bcbssc_sefl",
"bcbstn_uhs",
"bcbstx_phillips66",
"benefitfocus_sap",
"bridge_health",
"bswift_toyodagosei",
"bswift_xpo",
"businessolver",
"businessolver_gestamp",
"businessolver_marvell_us",
"businessolver_mcgrawhill",
"businessolver_schneider",
"businessolver_ssi",
"carters",
"carters_ameriben",
"cigna_equinix",
"cigna_universityofphoenix",
"cisco_alight",
"cityofaspen",
"construction_ind_lab",
"consumermedical",
"cooley",
"cort",
"cvs_cardinalhealth",
"cvs_danaher",
"cvs_motrex",
"cvs_phillips66",
"cvs_universityofphoenix",
"danaher_alight",
"dell_alight",
"eighthdistrict",
"empyrean_universityofphoenix",
"epic_toyodagosei",
"epson",
"estee_lauder_alight",
"fidelity_pepsico",
"finning",
"finningcat",
"fmcna",
"fmcp",
"gallagher_packsizes",
"gredeholdings",
"harborfreight_kaiser",
"healthcatalyst",
"healthequity_mskcc",
"hearst_empyrean",
"hines",
"hortongroup",
"humana",
"humana_cohere",
"included_health",
"indianalaborerswelfare",
"johnsonelectric",
"joyson",
"keeley",
"learning_care_group",
"learningcaregroup_aetna",
"lifeworks_concordia",
"local338",
"local440",
"logicalis_ADP",
"logicalis_mercer",
"marsh_mclennan",
"marshmma_clifbar",
"mastronardi",
"mckee_foods",
"mercer_pearson",
"meritain_govcio",
"motrex_motrex",
"mskcc",
"navitus",
"ncsrcc",
"oak_street_health",
"ohiolaborers",
"overstock_evernorth",
"overstock_overstock",
"paycom_flowcompanies",
"pinnacletriune_carolinadealerships",
"pipetradesindustry",
"plansource_lam_Research",
"potlatchdeltic",
"premera_potlatchdeltic",
"pvh",
"q2",
"schneider",
"shamrockfoods",
"swire_cocacola",
"swordhealth_perk",
"ttec_blo",
"turnerconstruction",
"ufcw_local338",
"ufp_aetna",
"uhc_danaher",
"uhc_generaldynamics",
"ukg_steeldynamics",
"us_cold_storage",
"us_cold_storage_mercer",
"weber",
"weebf",
"wood",
"wtw_uhs",
"accarent",
"accolade",
"amwell",
"bcbsil",
"cambia",
"castlight",
"cigna",
"compassrose",
"fidelity_generaldynamics",
"highmark",
"independencebluecross",
"medicalmutualofohio",
"nimble_health",
"rightway",
"springbuk_statefarm",
"surest",
"surgery_plus",
"transcarent",
"umr",
"virginpulse",]

# COMMAND ----------

l=["meritain_aaacluballiance"]

# COMMAND ----------

for i in list0:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%110%'")) 

# COMMAND ----------

for i in list1:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

for i in list2:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

for i in list3:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

for i in list:
  df.createOrReplaceTempView("elig") 
  display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%229%'")) 

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%64%'")) 

# COMMAND ----------

df.createOrReplaceTempView("elig")

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8) from elig where file_name like '%gravie%'"))

# COMMAND ----------

file_list=dbutils.fs.ls("gs://sword-sftp-production-us-private/gravie/outbound/claims/med")
file_paths = [file.path for file in file_list if file.name.endswith(".csv")]

file_paths

# COMMAND ----------

display(spark.sql("select distinct client_id, file_name from elig where client_id in (18)"))

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where substring(unique_id,1,8) like '%64%'")) 

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/danaher/outbound/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/danaher/outbound/eligibility/files/")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/danaher/outbound/claims/med")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/danaher/outbound/claims/rx")

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/uhc_danaher/outbound/eligibility/")

# COMMAND ----------

gs://sword-sftp-production-us-private/danaher_alight/outbound/eligibility/

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/danaher_alight/outbound/eligibility/files/")

# COMMAND ----------

gs://sword-sftp-production-us-private/danaher/outbound/claims/med/

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/danaher/outbound/claims/med/")

# COMMAND ----------

dbutils.fs.ls("gs://swordhealth-production-us-private/danaher_alight/outbound/claims/rx/")

# COMMAND ----------

df.createOrReplaceTempView("elig")

# COMMAND ----------

display(spark.sql("select distinct client_id, substring(unique_id,1,8),file_name from elig where file_name like '%i%'")) 

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private/gravie/outbound/claims/med/")


# COMMAND ----------

file_list =dbutils.fs.ls("gs://swordhealth-production-us-private")

# Print the file names and pat

# COMMAND ----------

dbutils.fs.ls("gs://sword-sftp-production-us-private")

# COMMAND ----------

for file in file_list:
  print(file.name)
  print(file.path)

# COMMAND ----------

import csv 

results = {}
basepath = "gs://sword-sftp-production-us-private/danaher_alight/outbound/eligibility/files"
file_list = dbutils.fs.ls(basepath)
file_paths = [file.path for file in file_list if file.name.endswith("csv")]
headers = []
results = {}
for filename in file_paths:
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("sep", "|") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .load(filename) \
        .limit(1)

# COMMAND ----------

results

# COMMAND ----------

import csv 

results = {}
basepath = "gs://sword-sftp-production-us-private/gravie/outbound/eligibility/files"
file_list = dbutils.fs.ls(basepath)
file_paths = [file.path for file in file_list if file.name.endswith("txt")]
headers = []
results = {}
for filename in file_paths:
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("sep", "|") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .load(filename) \
        .limit(1)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/danaher_alight/outbound/eligibility/files/*.csv")

# Get the list of column names
header = df.columns

# Print the header
print(header)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

n=len(df.columns)
print(n)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/danaher/outbound/claims/med")

# Get the list of column names
header = df.columns

# Print the header
print(header)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/gravie/outbound/eligibility/files/*.txt")

# Get the list of column names
header = df.columns

# Print the header
print(header)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Read the file(s) as a dataframe
df = spark.read.format("csv").option("header", "true").load("gs://sword-sftp-production-us-private/gravie/outbound/claims/med/")

# Get the list of column names
header = df.columns

# Print the header
print(header)
