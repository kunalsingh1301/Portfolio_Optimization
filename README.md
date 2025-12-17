from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import format_number, col
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder.appName("CallCentreAnalytics").getOrCreate()

# Define months and their abbreviations
months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

# Dummy numbers for total HK clients for each month
total_HK_clients = {
    '202501': 1748757, '202502': 1749000, '202503': 1750000,
    '202504': 1751000, '202505': 1752000, '202506': 1753000,
    '202507': 1754000, '202508': 1755000, '202509': 1756000,
    '202510': 1757000, '202511': 1758000
}

stacy_cnt = ['en', 'zh']
stacy_auth = ['pre', 'post']
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# Define schemas
schema = StructType([
    StructField("month", StringType(), False),
    StructField("total_contact", StringType(), True),
    StructField("volume", IntegerType(), True)
])

schema2 = StructType([
    StructField("month", StringType(), False),
    StructField("total_contact_postlogin", StringType(), True),
    StructField("volume", IntegerType(), True)
])

schema3 = StructType([
    StructField("month", StringType(), False),
    StructField("unique_contact_count", StringType(), True),
    StructField("volume", IntegerType(), True)
])

schema4 = StructType([
    StructField("month", StringType(), False),
    StructField("contact_ratio", StringType(), True),
    StructField("volume", FloatType(), True)
])

# Initialize lists to store results
data = []
total_client_post = []
uniq_channel_cust = []
contact_rat = []

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

for part, mnth in months.items():
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

    # Read Stacy data
    stacy_dfs = {}
    for cnt in stacy_cnt:
        for auth in stacy_auth:
            key = f'{mnth}_stacy_{cnt}_{auth}'
            path = file_paths_stacy[key]
            if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
                df = spark.read.csv(path, header=True, inferSchema=True)
                stacy_dfs[key] = df
            else:
                print(f"File not found: {path}")

    # Read IVR data
    ivr_dfs = {}
    for key, path in ivr_file_paths.items():
        if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
            df = spark.read.csv(path, header=True, inferSchema=True)
            ivr_dfs[key] = df
        else:
            print(f"IVR file not found: {path}")

    # Read Call and Chat data
    if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(callcc)):
        df_call_cc = spark.read.csv(callcc, header=True, inferSchema=True)
    else:
        df_call_cc = None
        print(f"Call file not found: {callcc}")

    if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(chat_cc)):
        df_chat_cc = spark.read.csv(chat_cc, header=True, inferSchema=True)
    else:
        df_chat_cc = None
        print(f"Chat file not found: {chat_cc}")

    # Process data
    count1 = sum([df.count() for df in stacy_dfs.values()])
    if df_call_cc:
        df_call_cc = df_call_cc.toDF(*(col.replace(' ', '_') for col in df_call_cc.columns))
        count2 = df_call_cc.count()
    else:
        count2 = 0

    if df_chat_cc:
        count3 = df_chat_cc.count()
    else:
        count3 = 0

    count4 = sum([df.count() for df in ivr_dfs.values()])

    count1_post = sum([df.count() for key, df in stacy_dfs.items() if 'post' in key])
    if df_call_cc:
        count2_post = df_call_cc.filter(df_call_cc["Verification_Status"] == "Pass").count()
    else:
        count2_post = 0

    if df_chat_cc:
        count3_post = df_chat_cc.filter(df_chat_cc["Pre/Post"] == "Postlogin").count()
    else:
        count3_post = 0

    count4_post = sum([df.filter(col("ONE_FA").isin(post_login_values)).count() for df in ivr_dfs.values()])

    all_stacy_usr_ids = []
    for key, df in stacy_dfs.items():
        if 'post' in key:
            if 'user_id' in df.columns:
                usr_ids = df.select("user_id").distinct().rdd.flatMap(lambda x: x).collect()
            elif 'customer_id' in df.columns:
                usr_ids = df.select("customer_id").distinct().rdd.flatMap(lambda x: x).collect()
            else:
                usr_ids = []
            all_stacy_usr_ids.extend(usr_ids)
    count_uniq_stacy = len(set(all_stacy_usr_ids))

    if df_call_cc:
        count_uniq_call = df_call_cc.select("Customer_No_(CTI)").distinct().count()
    else:
        count_uniq_call = 0

    if df_chat_cc:
        count_uniq_chat = df_chat_cc.select("REL ID").distinct().count()
    else:
        count_uniq_chat = 0

    all_ivr_rel_ids = []
    for key, df in ivr_dfs.items():
        rel_ids = df.select("REL_ID").distinct().rdd.flatMap(lambda x: x).collect()
        all_ivr_rel_ids.extend(rel_ids)
    count_uniq_ivr = len(set(all_ivr_rel_ids))

    if count1 > 0:
        data.append((part, "Stacy", count1))
    if count3 > 0:
        data.append((part, "Live Chat", count3))
    if count2 > 0:
        data.append((part, "Call", count2))
    if count4 > 0:
        data.append((part, "IVR", count4))

    if count1_post > 0:
        total_client_post.append((part, "Stacy", count1_post))
    if count3_post > 0:
        total_client_post.append((part, "Live Chat", count3_post))
    if count2_post > 0:
        total_client_post.append((part, "Call", count2_post))
    if count4_post > 0:
        total_client_post.append((part, "IVR", count4_post))

    if count_uniq_stacy > 0:
        uniq_channel_cust.append((part, "Stacy", count_uniq_stacy))
    if count_uniq_chat > 0:
        uniq_channel_cust.append((part, "Live Chat", count_uniq_chat))
    if count_uniq_call > 0:
        uniq_channel_cust.append((part, "Call", count_uniq_call))
    if count_uniq_ivr > 0:
        uniq_channel_cust.append((part, "IVR", count_uniq_ivr))

    total_HK_client = total_HK_clients[part]
    if count_uniq_stacy > 0:
        contact_rat.append((part, "Stacy", count_uniq_stacy / total_HK_client * 100))
    if count_uniq_chat > 0:
        contact_rat.append((part, "Live Chat", count_uniq_chat / total_HK_client * 100))
    if count_uniq_call > 0:
        contact_rat.append((part, "Call", count_uniq_call / total_HK_client * 100))
    if count_uniq_ivr > 0:
        contact_rat.append((part, "IVR", count_uniq_ivr / total_HK_client * 100))

# Define schemas
schema = ["month", "total_contact", "volume"]
schema2 = ["month", "total_contact_postlogin", "volume"]
schema3 = ["month", "unique_contact_count", "volume"]
schema4 = ["month", "contact_ratio", "volume"]

# Create DataFrames
df_client_contact_overview = spark.createDataFrame(data, schema)
df_client_contact_overview_postlogin = spark.createDataFrame(total_client_post, schema2)
df_uniq_channel_cust = spark.createDataFrame(uniq_channel_cust, schema3)
df_contact_ratio = spark.createDataFrame(contact_rat, schema4)

# Format and show results
df_client_contact_overview = df_client_contact_overview.withColumn("formatted_volume", format_number("volume", 0))
df_client_contact_overview_postlogin = df_client_contact_overview_postlogin.withColumn("formatted_volume", format_number("volume", 0))
df_uniq_channel_cust = df_uniq_channel_cust.withColumn("formatted_volume", format_number("volume", 0))
df_contact_ratio = df_contact_ratio.withColumn("percentage", format_number("volume", 2))

df_client_contact_overview.select("month", "total_contact", "formatted_volume").show(truncate=False)
df_client_contact_overview_postlogin.select("month", "total_contact_postlogin", "formatted_volume").show(truncate=False)
df_uniq_channel_cust.select("month", "unique_contact_count", "formatted_volume").show(truncate=False)
df_contact_ratio.select("month", "contact_ratio", "percentage").show(truncate=False)
































from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp, row_number
from pyspark.sql.window import Window

def unify_structure(df, channel_name, user_id_col, timestamp_col):
    return df.withColumnRenamed(user_id_col, "user_id") \
             .withColumnRenamed(timestamp_col, "timestamp") \
             .withColumn("channel", col(channel_name))
             
# Unify Stacy DataFrames
    for key, df in stacy_dfs.items():
        stacy_dfs[key] = unify_structure(df, "Stacy", "user_id", "start_time")

    # Unify IVR DataFrames
    for key, df in ivr_dfs.items():
        ivr_dfs[key] = unify_structure(df, "IVR", "REL_ID", "start_time")

    # Unify Call Center DataFrame
    if df_call_cc:
        df_call_cc_unified = unify_structure(df_call_cc, "Call", "Customer_No_(CTI)", "start_time")
    else:
        df_call_cc_unified = None

    # Unify Chat DataFrame (combining date and time columns)
    if df_chat_cc:
        df_chat_cc = df_chat_cc.withColumn("timestamp", concat_ws(" ", col("date"), col("time")))
        df_chat_cc_unified = unify_structure(df_chat_cc, "Chat", "REL ID", "timestamp")
    else:
        df_chat_cc_unified = None

    # Combine all unified DataFrames
    all_dfs_part = list(stacy_dfs.values()) + list(ivr_dfs.values())
    if df_call_cc_unified:
        all_dfs_part.append(df_call_cc_unified)
    if df_chat_cc_unified:
        all_dfs_part.append(df_chat_cc_unified)

    # Union all DataFrames for the current month
    if all_dfs_part:
        combined_df_part = all_dfs_part[0]
        for df in all_dfs_part[1:]:
            combined_df_part = combined_df_part.union(df)
        all_dfs.append(combined_df_part)

# Union all DataFrames for all months
if all_dfs:
    combined_df = all_dfs[0]
    for df in all_dfs[1:]:
        combined_df = combined_df.union(df)

# Convert timestamp to proper format
combined_df = combined_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Define window specification to order contacts by timestamp
window_spec = Window.partitionBy("user_id").orderBy("timestamp")

# Add row numbers to determine the order of contact
combined_df = combined_df.withColumn("order", row_number().over(window_spec))

# Filter for first, second, and third contacts
first_contact_df = combined_df.filter(col("order") == 1).select("user_id", "channel", "timestamp")
second_contact_df = combined_df.filter(col("order") == 2).select("user_id", "channel", "timestamp")
third_contact_df = combined_df.filter(col("order") == 3).select("user_id", "channel", "timestamp")

# Show results
first_contact_df.show(truncate=False)
second_contact_df.show(truncate=False)
third_contact_df.show(truncate=False)










Flow through across channels (for post login chat / call with customer number only)		
		
# of Cases Handled	Monthly Volume (Sep24)	Definition 
First Contacted by : Stacy	155,364 	Customer first reach channel
Live Chat	57,242 	First contact channel by a customer in a calendar month (e.g. customer first contact in Nov - only for Post login ) 
Call	261 	
IVR	97,861 	
		
Case Start with Stacy	Monthly Volume (Sep24)	
# of Cases	57,242 	how many contact start with stacy
# of Customers	46,552 	how many customer start with stacy
Repeated Rate	50%	
# of cases with follow up = 0	28,548 	After customer reach out to stacy, no 2nd chat/ call
1	12,778 	After customer reach out to stacy, find livechat / call
2	7,127 	
3+	8,789 	
		
2nd Contact Channel		
Stacy	7,855 	After customer reach out to stacy, come back again to Stacy
Live Chat	16,177 	After customer reach out to stacy, passed to livechat
Call	4,662 	After customer reach out to stacy, passed to Call
IVR		After customer reach out to stacy, passed to IVR
		
Case Start with Livechat (same rules for case 43)		
Case Start with Call(same rules for case 43)		
Case Start with IVR(same rules for case 43)		
