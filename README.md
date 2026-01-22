from pyspark.sql import functions as F
from functools import reduce

# =========================
# 0) CONFIG: your 5 uploads
# =========================
# Put the exact HDFS / ADLS / NAS paths where Hue uploads land
# Example:
# paths = {"wk1": "/user/kunal/uploads/wk1.csv", "wk2": "/user/kunal/uploads/wk2.csv", ...}

paths = {
    "wk1": "/path/to/wk1",  # wk1.csv or wk1.xlsx
    "wk2": "/path/to/wk2",
    "wk3": "/path/to/wk3",
    "wk4": "/path/to/wk4",
    "wk5": "/path/to/wk5",
}

# Column names in your file (based on screenshot)
COL_PSID = "PSID"
COL_ROLE = "Role"
COL_DAYS = "Days With Login"

# =========================
# 1) Reader that works for CSV by default
#    (Excel support depends on cluster package)
# =========================
def read_week(path):
    # Try CSV first
    try:
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(path))
        return df
    except Exception as e_csv:
        # Try Excel if spark-excel is installed on your cluster
        try:
            df = (spark.read.format("com.crealytics.spark.excel")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .option("dataAddress", "'Sheet1'!A1")
                  .load(path))
            return df
        except Exception as e_xlsx:
            raise Exception(f"Failed to read as CSV and Excel: {path}\nCSV error: {e_csv}\nExcel error: {e_xlsx}")

# =========================
# 2) Standardize each week to: psid, wkX_login_days
# =========================
def prep_week(week_key, path):
    df = read_week(path)

    # normalize column names (sometimes extra spaces)
    # We'll do safe renaming by trimming all column headers
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())

    # basic select + clean
    out = (df
        .select(
            F.col(COL_PSID).cast("string").alias("psid"),
            F.trim(F.col(COL_ROLE)).alias("role"),
            F.col(COL_DAYS).cast("int").alias("days_with_login")
        )
        .filter(F.col("psid").isNotNull() & (F.col("psid") != ""))
        .filter(F.upper(F.col("role")) == F.lit("RM"))
        # if duplicates exist for same PSID in a week, keep max login days
        .groupBy("psid")
        .agg(F.max("days_with_login").alias(f"{week_key}_login_days"))
    )
    return out

week_dfs = [prep_week(wk, p) for wk, p in paths.items()]

# =========================
# 3) Merge all weeks into single 6-column table
# =========================
summary = reduce(lambda a,b: a.join(b, "psid", "full"), week_dfs)

# Fill missing weeks with 0
for wk in paths.keys():
    summary = summary.withColumn(f"{wk}_login_days", F.coalesce(F.col(f"{wk}_login_days"), F.lit(0)))

# Order columns
cols = ["psid"] + [f"{wk}_login_days" for wk in ["wk1","wk2","wk3","wk4","wk5"] if wk in paths]
summary = summary.select(*cols)

# Show output (your 6 columns)
summary.show(50, truncate=False)

# =========================
# 4) KPIs (derived from the summary table)
# =========================
# Assume each week is 5 working days (change if needed)
DAYS_IN_WEEK = 5

total_rms = summary.select("psid").distinct().count()

kpi_rows = []
for wk in ["wk1","wk2","wk3","wk4","wk5"]:
    if f"{wk}_login_days" not in summary.columns:
        continue

    col = F.col(f"{wk}_login_days")

    # weekly login rate: % with >0 days
    weekly_login_rate = (summary.select(F.avg((col > 0).cast("double")).alias("v")).first()["v"])

    # daily login rate: total login-days / (RM count * days_in_week)
    daily_login_rate = (summary.select((F.sum(col) / (F.lit(total_rms) * F.lit(DAYS_IN_WEEK))).alias("v")).first()["v"])

    # active every day (all 5 days)
    active_every_day = (summary.select(F.avg((col == DAYS_IN_WEEK).cast("double")).alias("v")).first()["v"])

    kpi_rows.append((wk, total_rms, daily_login_rate, weekly_login_rate, active_every_day))

kpis = spark.createDataFrame(
    kpi_rows,
    ["week", "total_rms", "daily_login_rate", "weekly_login_rate", "active_every_day_rate"]
)

kpis.show(truncate=False)

# =========================
# 5) "Active every week of month" (across available weeks)
# =========================
week_cols = [c for c in summary.columns if c.endswith("_login_days")]

# count how many weeks exist (and treat >0 as "active in that week")
active_week_count = sum([(F.col(c) > 0).cast("int") for c in week_cols])
weeks_present = len(week_cols)

active_every_week_rate = (summary
    .select(F.avg((active_week_count == F.lit(weeks_present)).cast("double")).alias("rate"))
    .first()["rate"]
)

print("Active every week of month rate =", active_every_week_rate)

# =========================
# 6) (Optional) Save as a managed table for dashboard reuse
# =========================
# summary.write.mode("overwrite").saveAsTable("tmp_rm_monthly_login_summary")
