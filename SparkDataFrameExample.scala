import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkDataFrameExample {
  def main(spark: SparkSession): Unit = {
    val df = spark.read.table("sales")
    
    // Basic select and filter
    val result1 = df.select("product_id", "amount")
      .filter("amount > 1000")
      
    // Join with aggregation
    val result2 = df.select("sales.date", "customers.name") 
      .join("customers", "sales.customer_id = customers.id")
      .groupBy("date")
      .agg(sum("amount").as("total_sales"))
      .orderBy(desc("total_sales"))
      .limit(10)

      val result3 = df.select("sales.date", "customers.name") 
      .join("customers", "sales.customer_id = customers.id")
      .groupBy("date")
      .agg(sum("amount").as("total_sales"))

      val result4 = df.select("sales.date", "customers.name") 
      .join("customers", "sales.customer_id = customers.id")
      .groupBy("date")
      .orderBy(desc("total_sales"))
      .limit(10)
      
  }
}