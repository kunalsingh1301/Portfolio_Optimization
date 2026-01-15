def safe_read(func, path):
    try:
        if path_exists(path):
            df = func(path)
            if df is not None and not df.rdd.isEmpty():
                return df
        else:
            print(path + " doesn't exist")
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None

# ---------------- DATA READERS ----------------
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    df = df.filter(col(user_col).isNotNull()&(trim(col(user_col)) != ""))
    print("Stacy")
    print(df.count())
    df = normalize_timestamp(df, ts_col)
    return df.select(col(user_col).alias("user_id"),
                     "event_ts",
                     lit("Stacy").alias("channel"))

def read_ivr(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = df.filter(col("REL_ID").isNotNull()&(trim(col("REL_ID")) != ""))
    print("ivr")
    print(df.count())
    df = normalize_timestamp(df, "STARTTIME")
    return df.select(col("REL_ID").alias("user_id"),
                     "event_ts",
                     lit("IVR").alias("channel"))

def read_call(path, debug_path=None):
    df = spark.read.option("header", True).csv(path)
    df=df.filter(
      col("Customer No (CTI)").isNotNull()&(trim(col("Customer No (CTI)")) != "")
      )
    df = df.withColumn("raw_ts",col("Call Start Time"))
    df = df.filter(df["Verification Status"] == "Pass")
    print("call")
    print(df.count())
    
    df = normalize_timestamp(df, "Call Start Time")
    df.select( col("Customer No (CTI)").alias("user_id"),"raw_ts","event_ts").coalesce(1).write.mode("overwrite").csv(f"/user/2030435/CallCentreAnalystics/0001.csv")

    return df.select(col("Customer No (CTI)").alias("user_id"),
                     "event_ts",
                     lit("Call").alias("channel"))

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    df = df.filter(col("REL ID").isNotNull()&(trim(col("REL ID")) != ""))
    print("chat")
    print(df.count())
    df = normalize_timestamp(df, "_ts")
    return df.select(col("REL ID").alias("user_id"),
                     "event_ts",
                     lit("Chat").alias("channel"))

# ---------------- LOAD DATA ----------------
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = safe_read(read_stacy, p)
        if df: dfs.append(df)

for i in range(1, 5):
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    df = safe_read(read_ivr, p)
    if df: dfs.append(df)

# Call with debug CSV
call_debug_path = f"/user/2030435/CallCentreAnalystics/debug_call_{mnth}.csv"
df = safe_read(read_call,
               f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
if df: dfs.append(df)

df = safe_read(read_chat, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")
if df: dfs.append(df)

if not dfs:
    raise ValueError("No data found")
