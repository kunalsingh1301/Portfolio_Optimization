from pyspark.sql.functions import (
    col, trim, split, when,
    to_timestamp, coalesce,
    unix_timestamp, from_unixtime, lit
)

def normalize_timestamp(df, ts_col):

    # -------------------------------------------------
    # 1. Raw trimmed string
    # -------------------------------------------------
    df = df.withColumn("_raw_ts", trim(col(ts_col)))

    # -------------------------------------------------
    # 2. Detect Excel split numeric
    #    "45925 0.8766"
    # -------------------------------------------------
    df = df.withColumn(
        "_excel_days",
        when(
            col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"),
            split(col("_raw_ts"), r"\s+")[0].cast("double")
        )
    )

    df = df.withColumn(
        "_excel_fraction",
        when(
            col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"),
            split(col("_raw_ts"), r"\s+")[1].cast("double")
        )
    )

    # -------------------------------------------------
    # 3. Build Excel numeric value CORRECTLY
    # -------------------------------------------------
    df = df.withColumn(
        "_excel_value",
        when(
            col("_excel_days").isNotNull(),
            col("_excel_days") + col("_excel_fraction")
        )
    )

    # -------------------------------------------------
    # 4. Excel numeric → timestamp
    # -------------------------------------------------
    df = df.withColumn(
        "_excel_ts",
        when(
            col("_excel_value").isNotNull(),
            from_unixtime(
                unix_timestamp(lit("1899-12-30")) +
                (col("_excel_value") * 86400)
            )
        )
    )

    # -------------------------------------------------
    # 5. Normal string timestamps
    # -------------------------------------------------
    formats = [
        "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
        "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
        "d-M-yy HH:mm", "d-M-yy hh:mm a",
        "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",

        "M-d-yy HH:mm:ss", "M-d-yyyy HH:mm:ss",
        "M-d-yy HH:mm", "M-d-yyyy HH:mm",
        "M-d-yy HH:mm:ss a", "M-d-yyyy HH:mm:ss a",
        "M-d-yy HH:mm a", "M-d-yyyy HH:mm a",

        "dd/MM/yyyy HH:mm"
    ]

    df = df.withColumn(
        "_string_ts",
        coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats])
    )

    # -------------------------------------------------
    # 6. FINAL event_ts
    # -------------------------------------------------
    df = df.withColumn(
        "event_ts",
        when(col("_excel_ts").isNotNull(), col("_excel_ts"))
        .otherwise(col("_string_ts"))
    )

    return df.drop(
        "_raw_ts", "_excel_days", "_excel_fraction",
        "_excel_value", "_excel_ts", "_string_ts"
    )
