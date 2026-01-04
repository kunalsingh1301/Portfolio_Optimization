# Channel Flow Analysis Pipeline

## Overview

This project implements a **robust PySpark pipeline** to analyze customer interaction flows across multiple service channels (Stacy, IVR, Call, Chat). It is designed to handle **messy real-world data**, ensure correctness of metrics like repetition rate, and produce a **fixed reporting schema** suitable for downstream analytics or business reporting.

The pipeline is optimized for **Spark 3.x**, runs reliably in **Hue**, and gracefully handles missing or malformed files.

---

## Business Objective

Answer the following questions **per starter channel**:

1. How many total interactions occurred?
2. How many unique customers interacted?
3. What percentage of interactions are repeats?
4. How many users came back 0, 1, 2, or 3+ times?
5. What are the most common **2nd** and **3rd** channels users move to?

---

## Input Data Sources

| Source | Description                  | Key Columns                             |
| ------ | ---------------------------- | --------------------------------------- |
| Stacy  | App / Web interactions       | user_id / customer_id, HKT / date (UTC) |
| IVR    | IVR post-login flows         | REL_ID, STARTTIME                       |
| Call   | Call center interactions     | Customer No (CTI), Call Start Time      |
| Chat   | Chat post-login interactions | REL ID, Date28, StartTime               |

⚠️ Data issues handled:

* Day/month may be 1 or 2 digits
* Year may be 2 or 4 digits
* Time may be 24-hour with AM/PM
* Missing seconds
* Missing monthly files

---

## Output Schema

```text
Date
Channel
 total_case
 uniq_cust
 rep_rate
 follow_up_0
 follow_up_1
 follow_up_2
 follow_up_3+
 sec_chnl_1
 sec_chnl_2
 sec_chnl_3
 sec_chnl_4
 sec_chnl_count_1
 sec_chnl_count_2
 sec_chnl_count_3
 sec_chnl_count_4
 third_chnl_1
 third_chnl_2
 third_chnl_3
 third_chnl_4
 third_chnl_count_1
 third_chnl_count_2
 third_chnl_count_3
 third_chnl_count_4
```

---

## High-Level Architecture

```
            +-------------+
            |  Stacy CSV  |
            +-------------+
                   |
            +-------------+
            |   IVR CSV   |
            +-------------+       +-------------------+
                   |               | Timestamp         |
            +-------------+ -----> | Normalization     |
            |  Call CSV   |       +-------------------+
                   |
            +-------------+
            |  Chat CSV   |
            +-------------+
                   |
                   v
        +-------------------------+
        | Unified Event Table     |
        | (user_id, event_ts,     |
        |  channel)               |
        +-------------------------+
                   |
                   v
        +-------------------------+
        | Window Function         |
        | row_number() per user   |
        +-------------------------+
                   |
                   v
        +-------------------------+
        | Starter Channel Logic   |
        +-------------------------+
                   |
                   v
        +-------------------------+
        | Aggregations & Ranking  |
        +-------------------------+
                   |
                   v
        +-------------------------+
        | Final Report Output     |
        +-------------------------+
```

---

## Data Lineage (Detailed)

### Event-Level Lineage

```
Raw CSV
  |
  |-- read_*()
  |
Normalized Timestamp
  |
  |-- normalize_timestamp()
  |
(user_id, event_ts, channel)
  |
  |-- unionByName()
  |
Combined Event Table
  |
  |-- repartition(user_id)
  |-- row_number() OVER (user_id ORDER BY event_ts)
  |
Ranked Events
```

### Aggregation Lineage

```
Ranked Events
  |
  |-- rn = 1  --> Starter Channel
  |
  |-- groupBy(starter_channel)
  |        |-- total_case
  |        |-- uniq_cust
  |        |-- rep_rate
  |
  |-- groupBy(starter_channel, user_id)
  |        |-- follow_up buckets
  |
  |-- rn = 2 / rn = 3
           |-- top-N ranking per starter channel
```

---

## Key Design Decisions

### 1. Lazy Evaluation

All transformations (`select`, `withColumn`, `groupBy`) are **lazy**. Spark only executes when an action like `.show()` or `.count()` is called.

### 2. No UDFs

Built-in Spark functions are used exclusively for:

* Performance
* Catalyst optimization
* JVM-level execution

### 3. Window Functions

`row_number()` is used to:

* Identify starter channel
* Identify 2nd and 3rd interactions

This avoids expensive self-joins.

### 4. Defensive File Handling

* HDFS path existence checks
* Safe reads with try/except
* Empty DataFrames skipped gracefully

### 5. Fixed Output Schema

Final output is explicitly ordered and filtered to ensure **stable downstream consumption**, even if some channels are missing in a month.

---

## Performance Considerations

| Optimization         | Reason                   |
| -------------------- | ------------------------ |
| repartition(user_id) | Reduces window shuffle   |
| cache()              | Reused multiple times    |
| No collect() loops   | Avoids driver bottleneck |
| Top-N via window     | Distributed ranking      |

---

## Failure Scenarios & Handling

| Issue             | Handling                    |
| ----------------- | --------------------------- |
| Missing CSV       | Skipped safely              |
| Invalid timestamp | Parsed via fallback formats |
| Empty REL_ID      | Dropped via `dropna`        |
| Schema mismatch   | Column normalization        |

---

## When to Modify This Pipeline

* Add new channel → implement new `read_*()` function
* New timestamp format → add to `formats` list
* Different follow-up logic → update bucket mapping
* New ranking depth → adjust `rn` filter

---

## Summary

This pipeline is **production-grade**, scalable, and resilient to real-world data issues. It applies best practices from:

* Distributed systems
* Data engineering
* Spark performance tuning

It is suitable for **monthly batch reporting**, ad-hoc analysis, and long-term maintenance.

---

*Author: Internal Analytics Engineering*
