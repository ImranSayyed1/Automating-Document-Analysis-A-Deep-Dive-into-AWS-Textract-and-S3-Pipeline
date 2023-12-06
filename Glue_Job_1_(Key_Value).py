Glue Job

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json
from pyspark.sql.window import Window  # Import Window module
from pyspark.sql.functions import row_number, lit, to_date

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="project-test",
    table_name="json_clean_data",
    transformation_ctx="S3bucket_node1",
)

S3bucket_node1.printSchema()
S3bucket_node1.show(1)

# Script generated for node Change Schema
ChangeSchema_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("lineitems", "string", "lineitems", "string"),
        ("kv_data", "string", "kv_data", "string")
    ],
    transformation_ctx="ChangeSchema_node2",
)


df = ChangeSchema_node2.toDF()
print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Actual Data $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
df.printSchema()

# Define a window specification for row numbering
window_spec = Window.orderBy(lit(1))  # Order by a constant value

# Add a "PrimaryKey" column with continuous values starting from 1
df = df.withColumn("PrimaryKey", row_number().over(window_spec))
df.printSchema()#(n=2, truncate=False, vertical=True)


from pyspark.sql.functions import posexplode, col, expr
df.printSchema()
df=df.withColumn("kv_data", expr("from_json(kv_data, 'struct<Type:array<string>,Key:array<string>,Value:array<string>>')"))
df=df.withColumn("lineitems", expr("from_json(lineitems, 'struct<items:array<string>,price:array<string>,quantity:array<string>>')"))
df.printSchema()

df_exploded = df.select("PrimaryKey","kv_data.Type", "kv_data.Key", "kv_data.Value")#.withColumn("idx", posexplode("kv_data.Type"))

# test
from pyspark.sql.functions import expr, explode, col, collect_list, first, array

# Create an array column containing values from 'kv_data_Type', 'kv_data_Key', and 'kv_data_Value'
test1 = df_exploded.withColumn("kv_data_Array", array(col("Type"), col("Key"), col("Value")))

# Show the DataFrame with the new array column
test1.show(n=1,truncate=False, vertical=True)
test1.printSchema()

# test
###### from pyspark.sql.functions import udf
from pyspark.sql.functions import arrays_zip, col
from pyspark.sql.types import ArrayType, StringType
import re
# Combine the 'Type' and 'Key' columns using arrays_zip
def merge_lists(type_col, key_col):
    merged_list = []
    for t, k in zip(key_col, type_col):
        if k is None or k == "" or k.strip().lower() == "other":
            merged_list.append(t.upper())
        else:
            merged_list.append(k.upper())
    return merged_list

# Register the UDF
merge_lists_udf = udf(merge_lists, ArrayType(StringType()))

# Combine the 'Type' and 'Key' columns using arrays_zip
combined_columns = arrays_zip(test1["Type"], test1["Key"])

# Apply the UDF to create a new column 'merged_kv_data'
testing2 = test1.withColumn("merged_kv_data", merge_lists_udf(col("Key"), col("Type")))

# Show the DataFrame with the new merged column
testing2.show(n=1, truncate=False, vertical=True)


# test
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a UDF to recursively clean nested lists
def clean_nested_list(arr):
    def clean_element(elem):
        if isinstance(elem, list):
            return [clean_element(e) for e in elem]
        elif isinstance(elem, str):
            # Remove special characters and extra white spaces
            cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', elem)
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
            return cleaned_text
        else:
            return elem

    cleaned_arr = [clean_element(elem) for elem in arr]
    return cleaned_arr

# Register the UDF
clean_nested_list_udf = udf(clean_nested_list, ArrayType(StringType()))

# Apply the UDF to the "merged_kv_data" column
Clean = testing2.withColumn("merged_kv_data_cleaned", clean_nested_list_udf(testing2["merged_kv_data"]))

# Show the cleaned DataFrame
Clean.show(1, truncate=False, vertical=True)
Clean.printSchema()
print("clean data")


from pyspark.sql import functions as F

df_type =Clean.selectExpr("PrimaryKey", "inline(arrays_zip(merged_kv_data_cleaned, Value))")
df_Type = df_type.groupBy('PrimaryKey').pivot('merged_kv_data_cleaned').agg(F.first('Value'))
df_Type.printSchema()


sample=df_Type.join(Clean, on="PrimaryKey", how="inner").drop("merged_kv_data", "kv_data_Array", "Key", "Type", "value")
sample.printSchema()
print("Sample")
sample.show(1, truncate=False, vertical=True)

from pyspark.sql.functions import col, when, length

mobile_columns = [col_name for col_name in sample.columns if col_name.startswith("mobile") or col_name.startswith("MOBILE")]

if mobile_columns:
    done = sample.withColumn("MOBILE", when(length(col("MOBILE")) == 10, col(mobile_columns[0])).otherwise(col("MOBILE")))
else:
    # Handle the case where there are no matching columns
    done = sample




from pyspark.sql.functions import col, split, regexp_replace, udf, when
from pyspark.sql.types import StringType, IntegerType

selected_columns = ['PrimaryKey', 'ITEMS', 'PRICE', 'QUANTITY', 'TOTALAMOUNT', 'DATE', 'USER','SESSIONIDS', 'STOREIDS', 'TENANTIDS', 'ADDRESS', 'MOBILE']

# Create a new DataFrame with the selected columns
j = sample.select(selected_columns)
j = j.withColumn("ADDRESS", regexp_replace(col("ADDRESS"), "\n", " "))


print("****************************************************")
j.show(10, truncate=False, vertical=True)
print("****************************************************")



import ast
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession

# Define a UDF to remove '\n', special characters, and numeric values from a list of strings
def clean_item_list(item_list):
    cleaned_items = []
    for item in ast.literal_eval(item_list):
        # Remove newline characters
        cleaned_item = item.replace('\n', ' ')
        # Remove special characters and numeric values using regex
        cleaned_item = re.sub('[^a-zA-Z\s]', '', cleaned_item)
        cleaned_item = cleaned_item.upper()
        cleaned_item = re.sub('\s+', ' ', cleaned_item)
        cleaned_items.append(cleaned_item)
    return cleaned_items

# Register the UDF
spark.udf.register("clean_item_list_udf", clean_item_list, ArrayType(StringType()))

# Apply the UDF to the "ITEMS" column
df_clnd = j.withColumn("ITEMS", expr("clean_item_list_udf(ITEMS)"))

df_clnd.show(5, truncate=False, vertical=True)

Dp = df_clnd
Dp.show(3, truncate=False, vertical=True)

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
def extract_values(s):
    return re.findall(r'"(.*?)"', s)

# Register the UDF
extract_values_udf = udf(extract_values, ArrayType(StringType()))

# Apply the UDF to the PRICE column
DX = Dp.withColumn("PRICE", extract_values_udf("PRICE"))
DX = DX.withColumn("QUANTITY", extract_values_udf("QUANTITY"))
print("price length")
DX.show(3, truncate=False, vertical=True)
DX.printSchema()



from pyspark.sql.functions import size, col

dx = DX.withColumn("ITEMS_LENGTH", size(col("ITEMS")))
dx = dx.withColumn("PRICE_LENGTH", size(col("PRICE")))
dx = dx.withColumn("QUANTITY_LENGTH", size(col("QUANTITY")))
print("dx check print")
dx.show(5, truncate=False, vertical=True)
dx.printSchema()

# kv = dx
# dc=["ITEMS", "PRICE", "QUANTITY", "SESSIONIDS", "ITEMS_LENGTH", "PRICE_LENGTH", "QUANTITY_LENGTH"]
# kv.drop(*dc)
kv = dx.select("STOREIDS", "TENANTIDS", "SESSIONIDS", "USER", "DATE", "TOTALAMOUNT", "ADDRESS", "MOBILE")
print("key value")
from awsglue.dynamicframe import DynamicFrame
kv_dynamic_frame = DynamicFrame.fromDF(kv, glueContext, "kv_dynamic_frame")
kv_dynamic_frame.printSchema()
kv = ApplyMapping.apply(
    frame=kv_dynamic_frame,
    mappings=[
        ("TOTALAMOUNT", "string", "TOTAL_AMOUNT", "double"),
        ("STOREIDS", "string", "STOREID", "string"),
        ("TENANTIDS", "string", "TENANTIDS", "string"),
        ("SESSIONIDS", "string", "SESSIONID", "string"),
        ("USER", "string", "USER", "string"),
        ("DATE", "string", "date", "string"),
        ("ADDRESS", "string", "ADDRESS", "string"),
        ("MOBILE", "string", "MOBILE", "bigint")
    ]
)
kv.show()
kv.printSchema()


# Date mapping
kv = kv.toDF()
# Define the dictionary for month mapping
month_dict = {"jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
              "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"}

# Split the date_column by space, hyphen, and slash
df = kv.withColumn("date_list", split(col("date"), " |\\-|/"))
df.printSchema()
df.show(truncate=False, vertical=True)

# Define a list of days of the week to remove
days_to_remove = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
# months_str = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
months_str = ["weal"]

# Process the data
def transform_date_list(date_list):
    if date_list is None:
        return None
        
    cleaned_date_list = [x for x in date_list if x.lower() not in days_to_remove]
    cleaned_date_list = [x for x in cleaned_date_list if x.lower() not in  months_str]
    
    if len(cleaned_date_list) < 3:
        cleaned_date_list.clear()
    elif len(cleaned_date_list) == 3 and cleaned_date_list[0].lower() in month_dict:
        cleaned_date_list[0], cleaned_date_list[1] = cleaned_date_list[1], cleaned_date_list[0].lower()
        cleaned_date_list[1] = month_dict[cleaned_date_list[1]]
    elif len(cleaned_date_list) == 3 and cleaned_date_list[0].isnumeric() and cleaned_date_list[1].isnumeric() and cleaned_date_list[2].isnumeric() and len(cleaned_date_list[0]) == 2 and len(cleaned_date_list[2]) == 2:
        cleaned_date_list[2] = "20"+cleaned_date_list[2]
    elif len(cleaned_date_list) == 3 and cleaned_date_list[0].isnumeric() and cleaned_date_list[1].isnumeric() and cleaned_date_list[2].isnumeric() and len(cleaned_date_list[0]) == 4 and len(cleaned_date_list[2]) == 2:
        cleaned_date_list = cleaned_date_list[::-1]
    elif len(cleaned_date_list) == 4:
        cleaned_date_list[0], cleaned_date_list[1] = cleaned_date_list[1], cleaned_date_list[0].lower()
        cleaned_date_list[1] = month_dict[cleaned_date_list[1]]
        cleaned_date_list = cleaned_date_list[:3]
    if len(cleaned_date_list) == 3:
        cleaned_date_list = "-".join(cleaned_date_list)
    return cleaned_date_list
    
transform_date_list_udf = udf(transform_date_list, StringType())
df = df.withColumn("date_list", transform_date_list_udf(col("date_list")))

# Select the result as per your requirement
# df = df.select("date_list")
# df = df.filter(col("date").isNotNull())

# df.show(truncate=False, vertical=True)

# original date
# df = df.withColumnRenamed("date", "date_og")

# df = df.withColumn("date", col("date_list")).drop("date_list")
df = df.withColumn("date", to_date(col("date"), "dd-MM-yyyy"))

# mapped date
# df = df.withColumn("date", to_date(col("date_list"), "dd-MM-yyyy")).drop("date_list")
df = df.withColumnRenamed("date_list", "DATES")
df.printSchema()
df.select('DATES','date').show(500)
missing_count = df.filter(col("DATES").isNull()).count()

print("Missing values count in 'DATES' column:", missing_count)
print("-----------------------------------------------------------------------------------------------------------------------------------------")
df = df.withColumn("DATES", to_date(col("DATES"), "dd-MM-yyyy"))
df.printSchema()
df.select('DATES','date').show(500)
missing_count = df.filter(col("DATES").isNull()).count()

print("Missing values count in 'DATES' column:", missing_count)
df.show(truncate=False, vertical=True)
df = df.drop("date")
df = df.withColumnRenamed("USER", "NAME")
df. printSchema()
df.show(10, truncate=False, vertical=True)
# convert it back to glue context
datasource = DynamicFrame.fromDF(df, glueContext, "updated")
print("-----------------------------------------------------------------------------------------------------------------------------------------")
print("Date mapping end--->")

try:
    # Script generated for node Amazon Redshift
    AmazonRedshift_node1698303215703 = glueContext.write_dynamic_frame.from_options(
        frame=datasource,
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": "s3://aws-glue-assets-093085588310-ap-south-1/temporary/",
            "useConnectionProperties": "true",
            "dbtable": "public.keyvalue",
            "connectionName": "project-connect",
            "preactions": "CREATE TABLE IF NOT EXISTS public.keyvalue (TOTAL_AMOUNT DOUBLE PRECISION, STOREID VARCHAR, TENANTIDS VARCHAR, SESSIONID VARCHAR, NAME VARCHAR, DATES VARCHAR, ADDRESS VARCHAR, MOBILE BIGINT);",
        },
        transformation_ctx="AmazonRedshift_node1698303215703",
    )
    
except Exception as e:
    # Handle any other exceptions
    print(f"Error: {str(e)}")
print("lineitems")
Li = dx.select("ITEMS", "PRICE", "QUANTITY", "SESSIONIDS") # "ITEMS_LENGTH", "PRICE_LENGTH", "QUANTITY_LENGTH")

Li.printSchema()
Li.show(10)

# Write the DataFrame to S3 with overwrite mode
s3_bucket = "s3://project-extracted-data/data-from-glue/lineitem_raw/"
Li.write.mode("overwrite").parquet(s3_bucket)

from awsglue.dynamicframe import DynamicFrame

modified_dynamic_frame = DynamicFrame.fromDF(Li, glueContext, "modified_dynamic_frame")
# Repartition the DynamicFrame to a single partition
modified_dynamic_frame = modified_dynamic_frame.repartition(1)
glueContext.write_dynamic_frame.from_options(
    frame=modified_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://project-extracted-data/data-from-glue/lineitem_raw/",
    "partitionKeys": [],
    },
    # format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

print("Done------------------------------------>>>>>>>>>>>>>>>>>>>")


job.commit()
