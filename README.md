from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, lit, count, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

# Define month and its abbreviation
mnth = 'jan'
part = '202501'

# Define channels and file paths
stacy_cnt = ['en', 'zh']
stacy_auth = ['post']
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# Define schemas
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("channel_name", StringType(), True),
])

# Initialize Spark session
spark = SparkSession.builder.appName("CallCentreAnalytics").getOrCreate()

# Initialize Hadoop file system
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

# Construct file paths
file_paths_stacy = {}
for cnt in stacy_cnt:
    for auth in stacy_auth:
        file_key = f'{mnth}_stacy_{cnt}_{auth}'
        file_path = f"/user/2030435/CallCentreAnalystics/{file_key}login.csv"
        file_paths_stacy[file_key] = file_path

ivr_file_paths = {}
for i in range(1, 5):
    file_key = f'{mnth}_ivr{i}'
    file_path = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    ivr_file_paths[file_key] = file_path

callcc = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
chat_cc = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"

# Function to read and clean chat data
def read_chat_data(file_path):
    df_chat = spark.read.csv(file_path, header=True)
    df_chat = df_chat.filter(df_chat["Pre/Post"] == "Postlogin")
    df_chat = df_chat.withColumn("StartTime", substring(df_chat.StartTime, 1, 5))
    df_chat = df_chat.select("REL ID", "Date28", "StartTime").withColumnRenamed("REL ID", "user_id")
    df_chat = df_chat.withColumn("channel_name", lit("Chat"))
    return df_chat

def read_stacy_data(file_path):
    df = spark.read.csv(file_path, header=True)
    df = df.withColumn("HKT_date", substring(df.HKT, 1, 10))
    df = df.withColumn("HKT_time", substring(df.HKT, 12, 16))
    df = df.select("user_id", "HKT_date", "HKT_time")
    df = df.withColumn("channel_name", lit("Stacy"))
    return df

def read_call_data(file_path):
    df = spark.read.csv(file_path, header=True)
    df = df.withColumn("Call Start Time_date", substring(df["Call Start Time"], 1, 10))
    df = df.withColumn("Call Start Time_time", substring(df["Call Start Time"], 12, 16))
    df = df.select("Customer No (CTI)", "Call Start Time_date", "Call Start Time_time")
    df = df.withColumn("channel_name", lit("Call"))
    return df

def read_ivr_data(file_path):
    df = spark.read.csv(file_path, header=True)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = df.withColumn("STARTTIME_date", substring(df.STARTTIME, 1, 10))
    df = df.withColumn("STARTTIME_time", substring(df.STARTTIME, 12, 16))
    df = df.select("REL_ID", "STARTTIME_date", "STARTTIME_time")
    df = df.withColumn("channel_name", lit("IVR"))
    return df

# Read Stacy data
stacy_dfs = []
for cnt in stacy_cnt:
    for auth in stacy_auth:
        key = f'{mnth}_stacy_{cnt}_{auth}'
        if 'post' in key:
            path = file_paths_stacy[key]
            if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
                df = read_stacy_data(path)
                stacy_dfs.append(df)
            else:
                print(f"File not found: {path}")

# Read IVR data
ivr_dfs = []
for key, path in ivr_file_paths.items():
    if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
        df = read_ivr_data(path)
        ivr_dfs.append(df)
    else:
        print(f"IVR file not found: {path}")

# Read Call and Chat data
if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(callcc)):
    df_call_cc = read_call_data(callcc)
else:
    df_call_cc = None
    print(f"Call file not found: {callcc}")

if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(chat_cc)):
    df_chat_cc = read_chat_data(chat_cc)
else:
    df_chat_cc = None
    print(f"Chat file not found: {chat_cc}")

# Combine all data into a single DataFrame
df_combined = None
if stacy_dfs:
    df_combined = stacy_dfs[0]
    for df in stacy_dfs[1:]:
        df_combined = df_combined.union(df)

if ivr_dfs:
    if df_combined:
        for df in ivr_dfs:
            df_combined = df_combined.union(df)
    else:
        df_combined = ivr_dfs[0]
        for df in ivr_dfs[1:]:
            df_combined = df_combined.union(df)

if df_call_cc:
    if df_combined:
        df_combined = df_combined.union(df_call_cc)
    else:
        df_combined = df_call_cc

if df_chat_cc:
    if df_combined:
        df_combined = df_combined.union(df_chat_cc)
    else:
        df_combined = df_chat_cc

# Drop rows where user_id is null
if df_combined:
    df_combined = df_combined.dropna(subset=["user_id"])

    channels = ["Stacy", "Chat", "Call", "IVR"]

    for channel in channels:
        # Calculate the number of cases that start with the channel
        cases_start_channel = df_combined.filter(col("channel_name") == channel).agg(count("user_id").alias("cases_start_channel")).collect()[0]["cases_start_channel"]
        
        # Calculate the number of unique customers that start with the channel
        customers_start_channel = df_combined.filter(col("channel_name") == channel).select("user_id").distinct().count()
        
        # Calculate the repeated rate
        repeated_rate = (cases_start_channel - df_combined.filter(col("channel_name") == channel).dropDuplicates(["user_id"]).count()) / cases_start_channel
        
        # Calculate the follow-up cases
        follow_up_cases = df_combined.groupBy("user_id").agg(count("channel_name").alias("follow_up_count"))
        follow_up_cases = follow_up_cases.groupBy("follow_up_count").agg(count("user_id").alias("cases_count")).orderBy("follow_up_count")
        
        # Calculate the second contact channel
        window_spec = Window.partitionBy("user_id").orderBy("HKT_date", "HKT_time")
        df_second_contact = df_combined.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 2)
        second_contact_channel = df_second_contact.groupBy("channel_name").agg(count("user_id").alias("second_contact_cases"))
        
        # Display the results
        print(f"Analysis for {channel}:")
        print(f"Number of Cases that Start with {channel}: {cases_start_channel}")
        print(f"Number of Unique Customers that Start with {channel}: {customers_start_channel}")
        print(f"Repeated Rate for {channel}: {repeated_rate * 100:.2f}%")
        
        print(f"Follow-up Cases for {channel}:")
        follow_up_cases.show(1000)
        
        print(f"Second Contact Channel after {channel}:")
        second_contact_channel.show()
else:
    print("No data available for the month of January.")
