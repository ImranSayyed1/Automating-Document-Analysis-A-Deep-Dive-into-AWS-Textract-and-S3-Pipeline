Glue Job

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

import pandas as pd 
import re
import numpy as np

print("pandas imported as pd.")
df = pd.read_parquet("s3://project-extracted-data/data-from-glue/lineitem_raw")
df.head(10)
print(df)

column_names = ['ITEMS', 'PRICE', 'QUANTITY', 'SESSIONID']
# Create an empty DataFrame with column names
df1 = pd.DataFrame(columns=column_names)

item =[]
qnt = []
price = []
ssion = []
for i in range(len(df)):
    item.append(df.loc[i,'ITEMS'])
    qnt.append(df.loc[i,'PRICE'])
    price.append(df.loc[i,'QUANTITY'])
    ssion.append([df.loc[i,'SESSIONIDS']] * len(df.loc[i,'ITEMS']))

print(len(item))
print(len(price))
print(len(qnt))

df1["ITEMS"] = item
df1['PRICE'] = qnt
df1['QUANTITY'] = price
df1['SESSIONID'] = ssion

print(df1)
print(">>>>>>>>><<<<<<<<<<<<<")

df_exploded = df1.apply(lambda col: col.apply(pd.Series).stack()).reset_index(drop=True)

print(df_exploded)
print(df_exploded['PRICE'])
print(type(df_exploded['PRICE']))
print("after")
for i in range(len(df_exploded['PRICE'])):
    df_exploded['PRICE'][i] = df_exploded['PRICE'][i].replace(',','')

print(type(df_exploded['PRICE']))

# Replace NaN and non-numeric values in "PRICE" column with -1
df_exploded["PRICE"] = df_exploded["PRICE"].apply(lambda x: -1 if pd.isna(x) or not re.match(r'^\d+(\.\d{1,2})?$', str(x)) else float(re.sub(r'[^\d.]', '', str(x))))

# Replace empty strings and non-numeric values in "QUANTITY" column with -1
df_exploded["QUANTITY"] = df_exploded["QUANTITY"].apply(lambda x: -1 if x == '' or not re.match(r'^\d+(\.\d{1,2})?$', str(x)) else float(re.sub(r'[^\d.]', '', str(x))))

# Display the modified DataFrame
print(df_exploded)

spark_df = spark.createDataFrame(df_exploded)
dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_name")

dynamic_frame.printSchema()


# Script generated for node Amazon Redshift
dynamic_frame = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-093085588310-ap-south-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.lineitem",
        "connectionName": "project-connect",
        "preactions": "CREATE TABLE IF NOT EXISTS public.lineitem (ITEMS VARCHAR, PRICE DOUBLE PRECISION, QUANTITY DOUBLE PRECISION, SESSIONID VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1698303215703",
)

job.commit()
