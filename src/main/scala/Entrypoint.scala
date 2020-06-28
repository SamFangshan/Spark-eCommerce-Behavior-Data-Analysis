import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, substring, to_timestamp}

object Entrypoint {
  def main(args: Array[String]): Unit = {
    // create spark session
    val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("eCommerceAnalysis")
        .getOrCreate()

    // prepare spark dataframe
    val df: DataFrame = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("data/2019-Dec.csv")
      .withColumn("event_time", substring(col("event_time"), 0, 19))
      .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))

    // exploratory analysis
    val analyzer = ExploratoryAnalyzer(spark, df)

    analyzer.exploreEvents()
    analyzer.explorePurchases()
  }
}