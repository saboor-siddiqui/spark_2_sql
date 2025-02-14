import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkDataFrameExample {
  def main(spark: SparkSession): Unit = {
    

    // Define DataFrames
    def eosEventLogPropUs: DataFrame = {
      readCS3Data(spark,"axp-lumid.dw_anon","eos_event_log_prop_us")
      .filter("col("offr_id").isNotNull && col("cm13").isNotNull && col("event_ts").isNotNull")
      .select("cm13", "event_ts","event_ts","cmpn_id","offr_id","plcmnt_opp_id","srvc_call_cd","event_type_cd","event_score","mer_rank","srce_cd")
      .withColumnRenamed("plcmnt_opp_id","placement_opp_id")
      .withColumnRenamed("srvc_call_cd","service_call_code")
      .withColumnRenamed("srce_cd","source code")
      .withColumn("event_dt",to_date(col("event_ts")))

    }

    def georCtgy: DataFrame = {
      readCS3Data(
        spark,
        "axp-lumid.dw_anon",
        "gcor_ctgy")
        .groupBy("indus_info_offr_id")
        .agg(
          min("ctgy_ns") as "ctgy_na",
          min("subctgy_na") as "subctgy_ns"
        )
    }

    def georOffr: DataFrame = {
      readCS3Data(
        spark,
        "axp-lumid.dw_anon",
        "gcor_offr")
        .select("offr_nm", "offr_id")
       .distinct()
      
    }

    // Fetch data from ecp email mkt campaign table
    def ecpEmailMktCnpgn: DataFrame = {
      readCS3Data(
        spark,
        "axp-lumid.dw_anon",
        "ecp_email_mkt_chpon")
        .groupBy("sail_hist_id","offr_nm")
        .agg(min("offr_nm") as "capt_na" )
    }

    def riskPersAcctHist: DataFrame = {
      readCS3Data(
        spark,
        "axp-lumid.dw_anon",
        "risk_pers_acct_hist")
      ).filter("date_trunc("month", add_months(to_date(lit(config.startDate)), -1)) === col("acct_as_of_dt")")
        .groupBy("acct_cust_xref_id","acct_as_of_dt")
        .agg(
          min("acct_bus_unit_cd") as "business_unit_cdf",
          count("acct_bus_unit_cd") as "ind"
        )
    }

    def riskersAcctHist: DataFrame = {
      readCS3Data(spark, "axp-lumid.dw_anon", "risk_pers_acct_hist")
        .select("coll", "acct_sub_prd_cd", "acct_as_of_dt", 
                "acct_tenure_mnths_ct", "acct_cust_xref_id", "acct_credit_bureau_score")
        .withColumnRenamed("acct_as_of_dt", "acct_as_of_dt")
        .withColumnRenamed("acct_tenure_mnths_ct", "account_tenure")
        .withColumnRenamed("acct_cust_xref_id", "cust_xref_id")
        .withColumnRenamed("acct_credit_bureau_score", "fico_score")
        .distinct()
    }

def riskersAcctHistAggregated: DataFrame = {
      readCS3Data(spark, "axp-lumid.dw_anon", "risk_pers_acct_hist")
      .groupBy("cust_xref_id","acct_as_of_dt")
       .agg(
          min("coll") as "coll",
          min("acct_sub_prd_cd") as "small_acct_sub_prd_cd",
          min("acct_as_of_dt") as "first_acct_as_of_dt",)
  }
}