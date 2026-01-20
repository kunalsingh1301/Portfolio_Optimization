from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

spark = SparkSession.builder.appName("Call_DM").enableHiveSupport().getOrCreate()

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

def safe_read(func, path,mnth):
    try:
        if path_exists(path):
            df = func(path,mnth)
            if df is not None and not df.rdd.isEmpty():
                return df
        else:
            print(path + " doesn't exist")
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None
    
def read_call(path,mnth):
    df = spark.read.option("header", True).csv(path)
    df = df.withColumn("yearmonth",lit(mnth))
    return df.select(
        col("yearmonth"),
        col("Customer No (CTI)").alias("user_id"),
    )

dfs = []
for part,mnth in months.items():
  d = safe_read(read_call, f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv",mnth)
  if d is not None:
      dfs.append
call_df = []
call_df = reduce(lambda d1,d2: d1.unionByName(d2), dfs) if dfs else spark.createDataFrame([], StructType([]))
