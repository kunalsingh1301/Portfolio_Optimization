def normalize_timestamp(df, ts_col):
    if ts_col == "Call Start Time":
      df = df.withColumn("_ts", regexp_replace(trim(col(ts_col)),r"\s+"," "))
    
      # Step 1: Handle space-separated numeric Excel values (e.g., "45231 0.5625")
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
              regexp_replace(col("_ts"), r"(\d+)\s+(\d+(\.\d+)?)$",r"\1.\2")
          ).otherwise(col("_ts"))
      )
      
      # Step 2: Convert Excel numeric dates to timestamp (e.g., "45231.5625")
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+(\.\d+)?$"),
              from_unixtime(unix_timestamp(lit("1899-12-30"))+(col("_ts").cast("double")*86400))
          ).otherwise(col("_ts"))
      )
      
      # Step 3: Add missing ":00" seconds to timestamps
      # Only add ":00" if the timestamp has HH:MM format (one colon) but no seconds
      df = df.withColumn(
          "_ts",
          when(
              # Match: date + space + HH:MM (no AM/PM, no seconds)
              # Pattern ensures exactly one colon in the time part
              col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}$"),
              concat(col("_ts"), lit(":00"))
          ).when(
              # Match: date + space + HH:MM AM/PM (no seconds)
              # The space before AM/PM prevents matching if there's already ":00 AM"
              col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}\s(AM|PM)$"),
              concat(col("_ts"), lit(":00"))
          ).otherwise(col("_ts"))
      )
  
      # Step 4: Parse timestamp using multiple format patterns
      formats = [
          # DD-MM-YYYY patterns (important for days 13-31)
          "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy hh:mm:ss a",
          "dd-MM-yyyy HH:mm", "dd-MM-yyyy hh:mm a",
          "dd-MM-yy HH:mm:ss", "dd-MM-yy hh:mm:ss a",
          "dd-MM-yy HH:mm", "dd-MM-yy hh:mm a",
          
          # D-M-YYYY patterns (flexible day/month)
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          
          # Slash-based formats
          "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy hh:mm:ss a",
          "dd/MM/yyyy HH:mm", "dd/MM/yyyy hh:mm a",
          "dd/MM/yy HH:mm:ss", "dd/MM/yy hh:mm:ss a",
          "dd/MM/yy HH:mm", "dd/MM/yy hh:mm a",
          
          # MM-DD-YYYY patterns (month-first, US format)
          "M-d-yyyy HH:mm:ss", "M-d-yyyy hh:mm:ss a",
          "M-d-yyyy HH:mm", "M-d-yyyy hh:mm a",
          "M-d-yy HH:mm:ss", "M-d-yy hh:mm:ss a",
          "M-d-yy HH:mm", "M-d-yy hh:mm a",
          
          # More slash formats
          "M/d/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm:ss",
          "M/d/yyyy HH:mm", "MM/dd/yyyy HH:mm",
          "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
          "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
          "M/d/yy HH:mm:ss", "MM/dd/yy HH:mm:ss",
          "M/d/yy HH:mm", "MM/dd/yy HH:mm",
          "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
          "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",
          
          # Date-only formats
          "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy"
      ]
  
      df = df.withColumn(
          "event_ts",
          coalesce(*[to_timestamp(col("_ts"), f) for f in formats])
      )
  
      return df.drop("_ts")
      
    else:
      df = df.withColumn("_raw_ts", regexp_replace(trim(col(ts_col)),r"\s+"," "))
      
      # Excel numeric handling
      df = df.withColumn("_excel_days",
                         when(col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"),
                              split(col("_raw_ts"), r"\s+")[0].cast("double")))
      df = df.withColumn("_excel_fraction",
                         when(col("_excel_days").isNotNull(),
                              split(col("_raw_ts"), r"\s+")[1].cast("double")))
      df = df.withColumn("_excel_value",
                         when(col("_excel_days").isNotNull(), col("_excel_days")+col("_excel_fraction")))
      df = df.withColumn("_excel_ts",
                         when(col("_excel_value").isNotNull(),
                              expr("timestampadd(SECOND,cast((_excel_value-25569)*86400 as int), timestamp('1970-01-01'))")))
      
      # String timestamp formats
      formats = [
          # DD-MM-YYYY patterns
          "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy hh:mm:ss a",
          "dd-MM-yyyy HH:mm", "dd-MM-yyyy hh:mm a",
          "dd-MM-yy HH:mm:ss", "dd-MM-yy hh:mm:ss a",
          "dd-MM-yy HH:mm", "dd-MM-yy hh:mm a",
          
          # D-M-YYYY patterns
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          
          # Slash formats
          "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy hh:mm:ss a",
          "dd/MM/yyyy HH:mm", "dd/MM/yyyy hh:mm a",
          "dd/MM/yy HH:mm:ss", "dd/MM/yy hh:mm:ss a",
          "dd/MM/yy HH:mm", "dd/MM/yy hh:mm a",
          
          # US formats
          "M-d-yyyy HH:mm:ss", "M-d-yyyy hh:mm:ss a",
          "M-d-yyyy HH:mm", "M-d-yyyy hh:mm a",
          "M-d-yy HH:mm:ss", "M-d-yy hh:mm:ss a",
          "M-d-yy HH:mm", "M-d-yy hh:mm a",
          
          "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
          "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
          "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
          "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",
          
          "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy"
      ]
  
      df = df.withColumn("_string_ts", coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats]))
  
      df = df.withColumn("event_ts",
                         when(col("_excel_ts").isNotNull(), col("_excel_ts"))
                         .otherwise(col("_string_ts")))

      return df
