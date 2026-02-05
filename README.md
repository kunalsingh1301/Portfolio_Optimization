xx = hk_scpay_dbt_xbdr_txn_usd.filter((col("Transaction_Month") == '202512') & ((col("tran_cat") == "Branch") | (col("tran_cat") == "SWIFT")))\
                    .select("Transaction_Month","pmttp","subpmttp","chanlid","uetr","dbtrcust_id","transfer_amt_usd","tran_cat","dbtragt_bicfi","cdtragt_bicfi","scb_to_scb","segment")
#                     .groupBy("Transaction_Month","tran_cat","pmttp","subpmttp","chanlid","segment")
