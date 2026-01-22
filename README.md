from pyspark.sql import functions as F
from functools import reduce

# -------------------------
# CONFIG: file locations
# -------------------------
paths = {
    "wk1": "/path/to/wk1.csv",
    "wk2": "/path/to/wk2.csv",
    "wk3": "/path/to/wk3.csv",
    "wk4": "/path/to/wk4.csv",
    "wk5": "/path/to/wk5.csv",
}

# Column names (as in your Excel screenshot)
COL_PSID = "PSID"
COL_ROLE = "Role"
COL_DAYS = "Days With Login"

DAYS_IN_WEEK = 5  # change if needed


# -------------------------
# Helper: read + standardize one week
# Output: psid, wkX_login_days
# -------------------------
def load_week(week_key, file_path):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(file_path))

    # trim header names (sometimes Excel exports add spaces)
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())

    out = (df
        .select(
            F.col(COL_PSID).cast("string").alias("psid"),
            F.trim(F.col(COL_ROLE)).alias("role"),
            F.col(COL_DAYS).cast("int").alias("days_with_login")
        )
        .filter(F.col("psid").isNotNull() & (F.col("psid") != ""))
        .filter(F.upper(F.col("role")) == "RM")
        # if duplicates exist for same RM in a week -> keep max login days
        .groupBy("psid")
        .agg(F.max("days_with_login").alias(f"{week_key}_login_days"))
    )
    return out


# -------------------------
# 1) Build the 6-column summary table
# -------------------------
week_dfs = [load_week(wk, p) for wk, p in paths.items()]

summary = reduce(lambda a, b: a.join(b, "psid", "full"), week_dfs)

# fill missing with 0
for wk in paths.keys():
    summary = summary.withColumn(f"{wk}_login_days", F.coalesce(F.col(f"{wk}_login_days"), F.lit(0)))

summary = summary.select(
    "psid",
    "wk1_login_days", "wk2_login_days", "wk3_login_days", "wk4_login_days", "wk5_login_days"
)

summary.show(50, truncate=False)


# -------------------------
# 2) KPIs per week (optional)
# -------------------------
total_rms = summary.select("psid").distinct().count()

def week_kpis(wk):
    col = F.col(f"{wk}_login_days")
    return (summary
        .agg(
            F.lit(wk).alias("week"),
            F.lit(total_rms).alias("total_rms"),
            (F.sum(col) / (F.lit(total_rms) * F.lit(DAYS_IN_WEEK))).alias("daily_login_rate"),
            F.avg((col > 0).cast("double")).alias("weekly_login_rate"),
            F.avg((col == DAYS_IN_WEEK).cast("double")).alias("active_every_day_rate")
        )
    )

kpis = week_kpis("wk1").unionByName(week_kpis("wk2")).unionByName(week_kpis("wk3")).unionByName(week_kpis("wk4")).unionByName(week_kpis("wk5"))
kpis.show(truncate=False)


# -------------------------
# 3) Active every week of month (your updated logic)
# "if they login every week at least once they will be counted"
# -------------------------

week_cols = ["wk1_login_days", "wk2_login_days", "wk3_login_days", "wk4_login_days", "wk5_login_days"]

# Determine which weeks actually exist this month:
# a week "exists" if at least one RM has login_days > 0
week_exists_exprs = [F.max((F.col(c) > 0).cast("int")).alias(c) for c in week_cols]
exists_row = summary.agg(*week_exists_exprs).collect()[0]
weeks_present = [c for c in week_cols if exists_row[c] == 1]

print("Weeks present:", weeks_present)

# Count weeks where a RM logged in at least once (>0)
active_weeks_count = sum([(F.col(c) > 0).cast("int") for c in weeks_present])

# RM is "active every week" if they are active in ALL present weeks
active_every_week_rate = (summary
    .select(F.avg((active_weeks_count == F.lit(len(weeks_present))).cast("double")).alias("rate"))
    .first()["rate"]
)

print("Active every week of month rate =", active_every_week_rate)


# -------------------------
# 4) (Optional) Save results
# -------------------------
# summary.write.mode("overwrite").saveAsTable("tmp_rm_month_login_summary")
# kpis.write.mode("overwrite").saveAsTable("tmp_rm_weekly_kpis")
