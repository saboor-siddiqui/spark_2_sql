import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkDataFrameExample {
  def main(spark: SparkSession): Unit = {
    val df = spark.read.table("sales")
    
    // Basic select and filter
    val collibraPreHeaderIcmv = spark.read.table("collibra_header_icmv").select("offer_nm_incentive", "incentive_buckets","collibra_model_id", "roi_cutoff","crc_channel_cost","resp_model_id","myca_resp_model_id","crc_resp_model_id","start_date","end_date")
    .withColumnRenamed("offr_total_cost_am", "total_cost")
    .withColumnRenamed("em_resp_model_id", "email_model_id")
    .withColumnRenamed("tax_rt", "tax_rate")
    .withColumnRenamed("em_resp_model_id", "email_model_id")
      .filter("crc_channel_cost > 1000")
      
    // Join with aggregation
    val cspModelScoreCm11 = spark.read.table("csp_model_score_cm11").select("coll outp vert", "coll seere","offer_nm_incentive", "incentive_buckets","collibra_model_id", "roi_cutoff","cm11")
      .join("gstar_transactions", "csp_model_score_cm11.cm13 = gstar_transactions.cm13")
      .groupBy("cm11,coll outp vert,coll seere")
      .agg(count("cm11").as("total_customers"))
      .orderBy(desc("total_customers"))

      val cms_campaign_info_avg = spark.read.table("cms_campaign_info").select("mbr_key", "abr_id", "aud_creat_ts", "strt_ts", "mkt_cd","key_type","cm13","roi_cutoff"")
      .join("csp_model_score_cm11", "cms_campaign_info.cm13 = csp_model_score_cm11.cm13")
      .groupBy("cm11,mbr_key,mkt_cd")
      .agg(avg("roi_cutoff").as("avg_roi_cutoff"))
      .orderBy(desc("avg_roi_cutoff"))

      
  }
}