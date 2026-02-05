import dataiku, warnings, pandas as pd, re
from dataiku import spark as dkuspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pandas.tseries.offsets import MonthEnd, MonthBegin, BMonthEnd
from pyspark.sql import functions as F
def start_spark_session(app_name, spark_config:dict=None):
    """Creates the spark session with the provided configuration.
       The sessions these generally create are ones with dynamic allocation configured, to enable multiple users
       execute their code with enough computation resources.
       :param app_name: The name of the spark application (empty will fill in the Dataiku's display name).
       :param spark_config: The spark configuration to add to the created session.
       :return: A tuple of a spark session and a spark SQL context."""
    # Concatenate the user display name with the provided app name.
    user_display_name = dataiku.api_client().get_own_user().get_settings().settings["displayName"]
    app_name = f"{user_display_name}'s session" if app_name is None else f"{app_name} ({user_display_name})"
    # Sets the builder to YARN client mode, with fast-path enabled.
    spark_builder = SparkSession.builder.appName(app_name).master("yarn").enableHiveSupport()
    # The configuration to ensure that dynamic allocation is applied for every session by default.
    dynamic_allocation_configuration = {
        "spark.shuffle.service.enabled": "true",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.dynamicAllocation.minExecutors": 1,
        "spark.dynamicAllocation.maxExecutors": 24,
        "spark.sql.files.maxPartitionBytes": "536870912",
        "spark.executor.instances": 1,
        "tez.cancel.delegation.tokens.on.completion": "false",
        "spark.yarn.queue": 'sgz1-rb_data_journey_haas_prd'
    }
    for key, value in dynamic_allocation_configuration.items():
        spark_builder = spark_builder.config(key, value)
    # Add any additional spark configurations.
    if spark_config is not None and isinstance(spark_config, dict):
        for key, value in spark_config.items():
            spark_builder = spark_builder.config(key, value)
    # Creates the spark session.
    spark = spark_builder.getOrCreate()
    sql_context = SQLContext(spark.sparkContext)
    return spark, sql_context
spark, sql_context = start_spark_session(app_name="X-Border")
# spark = SparkSession.builder.appName("X-Border").enableHiveSupport().getOrCreate()
spark
from pyspark.sql.window import Window
#Date Info
process_date_start = "20250601"
process_date_end   = "20251231"

trans_eff_start = "2023-12"
trans_eff_end   = "2025-12"

ods_start   = "2023-12-01"
ods_end     = "2025-12-31"

txn_mth = ["202312","202401","202402","202403","202404","202405",
           "202406","202407","202408","202409","202410","202411",
           "202412","202501","202502","202503","202504","202505",
           "202506","202507","202508","202509","202510","202511","202512"]

txn_mth_sql = ("202312","202401","202402","202403","202404","202405",
               "202406","202407","202408","202409","202410","202411",
               "202412","202501","202502","202503","202504","202505",
               "202506","202507","202508","202509","202510","202511","202512")


### Table Info 
# cc_table = "PRD_TB_adm_grp.crd_tran"


## Variable Info
country_code = "HK"
# data_src_cd  = "CCM"
exchg_rt_raw = spark.table("prd_vw_adm_grp_nsen.vw_exch_rt")\
                .filter((col("c_toccy") == "USD")
                      & (col("process_date") >= "20231201")   
                       )\
                .select(col("c_frccy").alias("ccy")
                        ,format_number("r_frto_ccy",4).alias("usd_fx")
                        ,format_number((1/col("r_frto_ccy")),4).alias("ccy_fx")
                        ,"process_date"
                        ,substring(col("process_date"),1,6).alias("trans_month")
                        ).distinct()
# exchg_rt_raw.show(3)
exchg_rt_raw.printSchema()
# b = exchg_rt_raw.filter(col("trans_month") == "202512")
# b.show(30)
xx = spark.table("PRD_TB_ADM_GRP.CASA_ACCT")
# xx.select(col("source_country_code")).distinct().show()
xx.printSchema()
# xx.filter(col("source_country_code")=="HK").groupBy("data_src","mnthend_fl").agg(
#     count("*")
# ).show(100)
# xx.select(col("source_country_code")).distinct().show()
hk_casa_acc = spark.table("PRD_TB_ADM_GRP.CASA_ACCT")\
                    .filter((col("process_date").between(process_date_start,process_date_end))
                            & (col("data_src") == "HOG")
                            & (col("mnthend_fl") == 'Y')
                            & (col("source_country_code") == 'HK')
                           )\
                    .select(col("n_acct").alias("acct_id")
                                  ,"x_glcusttyp"
                                  ,"c_acctccy"
                                  ,"x_prd"
                                  ,"c_dom_prd"
                                  ,"n_cust"
                                    ,"n_acct"
                            
                                  ,from_unixtime(unix_timestamp(col("process_date"),'yyyyMMdd'),'yyyMM').alias("yearmonth")
                                 ).distinct()
                    
# hk_casa_acc.show(5,truncate = False)
# hk_casa_acc.select("x_glcusttyp").distinct().show()
window_spec = Window.partitionBy("n_acct","yearmonth").orderBy(col("yearmonth").desc())
hk_casa_acc = hk_casa_acc.withColumn("row_number",row_number().over(window_spec))
hk_casa_acc_dup = hk_casa_acc.filter(col("row_number")==1)
# hk_casa_acc_dup.show(100,truncate = False)
hk_casa_acc_dup.printSchema()
xx = spark.table("prd_vw_sri_scpay_hk_tkn.actv_nonf_pmt_txndata")
xx.printSchema()
hk_scpay_txn = spark.table("prd_vw_sri_scpay_hk_tkn.actv_nonf_pmt_txndata")\
                    .select("uetr",
                            "intrbksttlmdt",
                            from_unixtime(unix_timestamp(col("intrbksttlmdt"),'yyyy-MM-dd'),'yyyMM').alias("Transaction_Month"),
                            "pmttp",
                            "subpmttp",
                            "chanlid",
                            "amt_intrbksttlmamt",
                            "amt_intrbksttlmamtccy",
                            "*"
                           )
# hk_scpay_txn = hk_scpay_txn.filter(col("Transaction_Month")==mnth)\
#                     .filter(col("pmttp")=="TT")\
#                     .filter(col("subpmttp").isin(["OT","IT"]))
hk_scpay_txn = hk_scpay_txn.select("*",
                                   when((trim(upper(col("chanlid"))) == "IBK") & (col("pmttp") == "TT") & (col("subpmttp") == "OT"),'SWIFT')
                                   .when((trim(upper(col("chanlid"))) == "RCP"),'Branch').otherwise("NA").alias("tran_cat")
                                  )
hk_scpay_dbt = spark.table("prd_vw_sri_scpay_hk_tkn.actv_nonf_pmt_txndata_dbt_inf")\
                    .select("uetr",
                            "pmttp",
                            "subpmttp",
                            "pmtid_custref",
                           "dbtragt_bicfi",
#                             substring(col("dbtragt_bicfi"),1,4).alias("transfer_from_bank"),
#                             substring(col("dbtragt_bicfi"),5,2).alias("transfer_from_country"),
                            "dbtracct_id",
                            from_unixtime(unix_timestamp(col("svcenddttm"),'yyyy-MM-dd'),'yyyMM').alias("Transaction_Month"),
                            "txevtsts"
                           )
# hk_scpay_dbt = hk_scpay_dbt.withColumn("direction",lit("OUT")).dropDuplicates(["uetr"])
# hk_scpay_dbt = hk_scpay_dbt.filter(col("Transaction_Month")==mnth)
# hk_scpay_dbt.agg( count("*").alias("no_obs"),
#                             countDistinct("uetr").alias("no_txn")
#                             ).show()
# hk_scpay_dbt.show(5,truncate = False)
hk_scpay_cdt = spark.table("prd_vw_sri_scpay_hk_tkn.actv_nonf_pmt_txndata_cdt_inf")\
                    .select("uetr",
                            "pmttp",
                            "subpmttp",
                            "pmtid_custref",
                           "cdtragt_bicfi",
#                             substring(col("dbtragt_bicfi"),1,4).alias("transfer_from_bank"),
#                             substring(col("dbtragt_bicfi"),5,2).alias("transfer_from_country"),
                            "cdtracct_id",
                            from_unixtime(unix_timestamp(col("svcenddttm"),'yyyy-MM-dd'),'yyyMM').alias("Transaction_Month"),
                            "txevtsts"
                           )
# hk_scpay_crd = hk_scpay_crd.withColumn("direction",lit("IN")).dropDuplicates(["uetr"])
# hk_scpay_crd = hk_scpay_crd.filter(col("Transaction_Month")==mnth)
# hk_scpay_crd.agg(count("*").alias("no_obs"),
#                             countDistinct("uetr").alias("no_txn")
#                             ).show()
# hk_scpay_cdt.select(col("txevtsts")).distinct().show()
# hk_scpay_cdt.show(5,truncate = False)
hk_scpay_cdt = hk_scpay_cdt.dropDuplicates(["uetr"])
hk_scpay_dbt = hk_scpay_dbt.dropDuplicates(["uetr"])
# hk_scpay_dbt.filter(col("dbtracct_id") == "8XHebwgrihd9R0F").show()
# hk_scpay_dbt.filter(col("dbtracct_id") == "00BoJFV97fmfK57D").show()
# hk_scpay_cdt.filter(col("cdtracct_id") == "00BoJFV97fmfK57D").show()
# spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
hk_scpay_dbt = hk_scpay_dbt.join(hk_casa_acc_dup,
                                 (hk_scpay_dbt.dbtracct_id == (hk_casa_acc_dup.n_acct))
                                 & (hk_scpay_dbt.Transaction_Month == hk_casa_acc_dup.yearmonth),
                                 how = 'left'
                                )\
                            .select(hk_scpay_dbt["*"],hk_casa_acc_dup["n_acct"].alias("dbtrcust_id"),hk_casa_acc_dup["x_glcusttyp"].alias("segment"))
# temp_df = hk_scpay_dbt
# temp_df = temp_df.dropna(subset = ["dbtrcust_id","segment"])
# temp_df.show(truncate = False)
# hk_scpay_dbt.show(5,truncate = False)
# hk_scpay_txn.filter((col("Transaction_Month") >= '202505'))\
#                          .select("Transaction_Month","pmttp","subpmttp","chanlid","uetr")\
#                          .groupBy("Transaction_Month","pmttp","subpmttp","chanlid")\
#                          .agg(
#                             count("*").alias("no_obs"),
#                             countDistinct("uetr").alias("trans_count"),
#                             )\
#                          .orderBy("Transaction_Month")\
#                          .show(1000,truncate=False)
# hk_scpay_dbt.filter((col("Transaction_Month") >= '202505')
#                     &(col("txevtsts")=="Success")
#                    )\
#                          .select("Transaction_Month","pmttp","subpmttp","uetr")\
#                          .groupBy("Transaction_Month","pmttp","subpmttp")\
#                          .agg(
#                             count("*").alias("no_obs"),
#                             countDistinct("uetr").alias("trans_count")
#                             )\
#                          .orderBy("Transaction_Month")\
#                          .show(10,truncate=False)
# hk_scpay_cdt.filter((col("Transaction_Month") >= '202505')
#                     &(col("txevtsts")=="Success")
#                    )\
#                          .select("Transaction_Month","pmttp","subpmttp","uetr")\
#                          .groupBy("Transaction_Month","pmttp","subpmttp")\
#                          .agg(
#                             count("*").alias("no_obs"),
#                             countDistinct("uetr").alias("trans_count")
#                             )\
#                          .orderBy("Transaction_Month")\
#                          .show(10,truncate=False)
hk_scpay_dbt_xbdr_txn = hk_scpay_txn\
                                .join(hk_scpay_dbt,
                                     (hk_scpay_txn.uetr == hk_scpay_dbt.uetr) &
                                      (hk_scpay_txn.pmttp == hk_scpay_dbt.pmttp) &
                                      (hk_scpay_txn.subpmttp == hk_scpay_dbt.subpmttp) &
                                      (hk_scpay_txn.Transaction_Month == hk_scpay_dbt.Transaction_Month),
                                      how='left'
                                     ).select(hk_scpay_txn["*"],
                                             hk_scpay_dbt["dbtracct_id"],
                                             hk_scpay_dbt["dbtrcust_id"],
                                             hk_scpay_dbt["dbtragt_bicfi"],
                                             hk_scpay_dbt["segment"] 
                                             )
hk_scpay_dbt_xbdr_txn = hk_scpay_dbt_xbdr_txn\
                                .join(hk_scpay_cdt,
                                     (hk_scpay_txn.uetr == hk_scpay_cdt.uetr) &
                                      (hk_scpay_txn.pmttp == hk_scpay_cdt.pmttp) &
                                      (hk_scpay_txn.subpmttp == hk_scpay_cdt.subpmttp) &
                                      (hk_scpay_txn.Transaction_Month == hk_scpay_cdt.Transaction_Month),
                                      how='left'
                                     ).select(hk_scpay_dbt_xbdr_txn["*"],
                                             hk_scpay_cdt["cdtragt_bicfi"],
                                             )
# hk_scpay_dbt_xbdr_txn.printSchema()
# hk_scpay_dbt_xbdr_txn.filter(col("Transaction_Month")>="202505").show(2,truncate = False)
# hk_scpay_dbt_xbdr_txn.filter((col("Transaction_Month") >= '202505'))\
#                          .select("Transaction_Month","pmttp","subpmttp","chanlid","uetr","dbtrcust_id")\
#                          .groupBy("Transaction_Month","pmttp","subpmttp","chanlid")\
#                          .agg(
#                             count("*").alias("no_obs"),
#                             countDistinct("uetr").alias("trans_count"),
#                             countDistinct("dbtrcust_id").alias("cust_count")
#                             )\
#                          .orderBy("Transaction_Month")\
#                          .show(1000,truncate=False)
hk_scpay_dbt_xbdr_txn = hk_scpay_dbt_xbdr_txn.select("*",
                                                     when((substring(col("cdtragt_bicfi"),1,3) == "SCB"),1).otherwise(0).alias("scb_to_scb")
                                                    )
hk_scpay_dbt_xbdr_txn_usd = hk_scpay_dbt_xbdr_txn.join(
    exchg_rt_raw, ((hk_scpay_dbt_xbdr_txn.Transaction_Month == exchg_rt_raw.trans_month)
                   & (hk_scpay_dbt_xbdr_txn.amt_intrbksttlmamtccy == exchg_rt_raw.ccy)
                  ), how = "left"
)\
.select(hk_scpay_dbt_xbdr_txn["*"]
        ,exchg_rt_raw["usd_fx"]
        ,when( (coalesce(exchg_rt_raw["ccy"],lit("0")) == "0")
              | (length(exchg_rt_raw["ccy"]) == 0),"Missing")
              .otherwise("Ref_Table").alias("ExcRateSource")
        ,when(
            (coalesce(exchg_rt_raw["ccy"], lit("0")) == "0") 
                                                 | (length(exchg_rt_raw["ccy"]) == 0), (hk_scpay_dbt_xbdr_txn["amt_intrbksttlmamtccy"] * 1))
                                           .otherwise((hk_scpay_dbt_xbdr_txn["amt_intrbksttlmamt"] * exchg_rt_raw["usd_fx"])).alias("transfer_amt_usd")
        )
       
 
 
xx= hk_scpay_dbt_xbdr_txn_usd.filter((col("Transaction_Month") >= '202505'))\
                         .select("Transaction_Month","pmttp","subpmttp","chanlid","uetr","dbtrcust_id","transfer_amt_usd","tran_cat","scb_to_scb")\
                         .groupBy("Transaction_Month","tran_cat","pmttp","subpmttp","chanlid","scb_to_scb")\
                         .agg(
                            count("*").alias("no_obs"),
                            countDistinct("uetr").alias("trans_count"),
                            countDistinct("dbtrcust_id").alias("cust_count"),
                            sum("transfer_amt_usd").alias("trans_usd")
                            )\
                         .orderBy("Transaction_Month")
hk_scpay_dbt_xbdr_txn_usd.filter((col("Transaction_Month") == '202512') & ((col("tran_cat") == "Branch") | (col("tran_cat") == "SWIFT")))\
                         .select("Transaction_Month","pmttp","subpmttp","chanlid","uetr","dbtrcust_id","transfer_amt_usd","tran_cat","scb_to_scb","segment")\
                         .groupBy("Transaction_Month","tran_cat","pmttp","subpmttp","chanlid","scb_to_scb","segment")\
                         .agg(
                            count("*").alias("no_obs"),
                            countDistinct("uetr").alias("trans_count"),
                            countDistinct("dbtrcust_id").alias("cust_count"),
                            sum("transfer_amt_usd").alias("trans_usd")
                            )\
                         .orderBy("Transaction_Month")\
                         .show(10000,truncate=False)
# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Output_df = xx
# Output = dataiku.Dataset("Output")
# dkuspark.write_with_schema(Output, Output_df)
hk_scpay_dbt_xbdr_txn_usd.filter((col("Transaction_Month")=="202512")
                                 &(col("tran_cat").isin("Branch","SWIFT"))
                                 &(col("pmttp").isin("TT"))
                                 &(col("subpmttp").isin("OT","BT"))
                                 &(col("chanlid").isin("SWT","IBK"))
                                 &(col("segment").isin("Premium Clients","Priority Clients"))
                                ).groupBy("Transaction_Month").agg(
    count(col("uetr")),
    sum(col("transfer_amt_usd"))
).show(truncate=False)
hk_scpay_dbt_xbdr_txn_usd.filter((col("Transaction_Month")=="202512")
                                 &(col("tran_cat").isin("Branch","SWIFT"))
                                 &(col("pmttp").isin("TT"))
                                 &(col("subpmttp").isin("OT","BT"))
                                 &(col("chanlid").isin("SWT","IBK"))
                                 &(col("scb_to_scb")==1)
                                 &(col("segment").isin("Premium Clients","Priority Clients"))
                                ).groupBy("Transaction_Month").agg(
    count(col("uetr")),
    sum(col("transfer_amt_usd"))
).show(truncate = False)
 
l6m_actv = hk_scpay_dbt_xbdr_txn_usd.filter(
    (col("Transaction_Month").between("202507","202512")) 
    & ((col("tran_cat") == "Branch") | (col("tran_cat") == "SWIFT")) 
    & ((col("segment") == "Premium Clients")| (col("segment") == "Priority Clients"))
).select("dbtrcust_id","scb_to_scb")
l6m_actv.select(countDistinct("dbtrcust_id")).show()
# l6m_actv.filter(col("scb_to_scb")==1).select(countDistinct("dbtrcust_id")).show()
# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs as SparkSQL dataframes
# Output_df = ... # Compute a SparkSQL dataframe to write into Output

# # Write recipe outputs
# Output = dataiku.Dataset("Output")
# dkuspark.write_with_schema(Output, Output_df)
