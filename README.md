from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import builtins
from functools import reduce

# Initialize Spark Session
spark = SparkSession.builder.appName("CallCentreAnalytics").getOrCreate()

months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

# ---------------- HDFS HELPERS ----------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

#-------------------------------------------------------------------------------------

def read_call(path):
    df = spark.read.option("header", True).csv(path)
    return df.select(col("Customer No (CTI)").alias("user_id"),
                     "event_ts",
                     lit("Call").alias("channel"))

def read_chat(path, part):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("mnth", lit(part))
    return df.select(col("REL ID").alias("user_id"), col("mnth"), col("NumericId").alias("num_id")).orderBy(col("num_id"))

def safe_read(func, part, path):
    try:
        if path_exists(path):
            df = func(path, part)
            if df is not None and not df.rdd.isEmpty():
                return df
        else:
            print(f"{path} doesn't exist")
    except AnalysisException as e:
        print(f"AnalysisException: {e}")
    except Exception as e:
        print(f"Exception: {e}")
    return None

def read_chat_nature(path, part):
    df = spark.read.option("header", "false").option("inferSchema", "true").csv(path)

    new_header = df.limit(3).collect()[2]
    df_data = df.rdd.zipWithIndex() \
        .filter(lambda x: x[1] > 3) \
        .map(lambda x: x[0]) \
        .toDF()
    df_final = df_data.toDF(*new_header)
    df_final = df_final.withColumn("mnth", lit(part))
    return df_final.select(col("Numeric ID").alias("num_id"), col("Combine").alias("nature"), col("mnth")).orderBy(col("num_id"))

dfs = []
dfcn = []

for part, mnth in months.items():
    # call_cc = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    chat_cc = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
    chat_nature_cc = f"/user/2030435/CallCentreAnalystics/{mnth}_chat_nature.csv"

    # df = safe_read(read_call, part, call_cc)
    # if df: dfs.append(df)

    df = safe_read(read_chat, part, chat_cc)
    if df: dfs.append(df)

    df = safe_read(read_chat_nature, part, chat_nature_cc)
    if df: dfcn.append(df)

if dfs:
    combined_df = reduce(lambda df1, df2: df1.union(df2), dfs)
else:
    combined_df = spark.createDataFrame([], StructType([]))

if dfcn:
    combined_df_nature = reduce(lambda df1, df2: df1.union(df2), dfcn)
else:
    combined_df_nature = spark.createDataFrame([], StructType([]))

final_df = combined_df.join(combined_df_nature, on=["num_id", "mnth"], how="left")

grouped_df = final_df.groupBy("user_id").agg(
    count("*").alias("contact_count"),
    collect_set("nature").alias("natures"),
    collect_list("mnth").alias("months")
)
multiple_contacts_df = grouped_df.filter(col("contact_count") > 1)
multiple_contacts_df.show(10,truncate=False)
BASE_PATH = f"/user/2030435/CallCentreAnalystics/ChatNatureDF.csv"

#multiple_contacts_df.coalesce(1) \
#    .write.mode("overwrite") \
#    .option("header", True) \
#    .csv("{BASE_PATH}/ChatNatureDF.csv")
