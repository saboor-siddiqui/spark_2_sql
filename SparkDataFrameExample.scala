import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkDataFrameExample {
  def main(spark: SparkSession): Unit = {
    

    // Define DataFrames
    def cedlCardMain: DataFrame = {
      readCS3Data(spark,"axp-lumid.dw_anon","cmdl_card_main").filter("trim(col("ctry_cd")) === "US" && col("cif") === "y" && col("cust_cref_id").isNotNull").select("col1", "col2").distinct()
    }

    def georCtgy: DataFrame = {
      readCS3Data(
        spark,
        "axp-lumid.dw_anon",
        "gcor_ctgy")
        .groupBy("indus_info_offr_id","ctgy_ns")
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
        .select("col1", "col2")
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

  }
}