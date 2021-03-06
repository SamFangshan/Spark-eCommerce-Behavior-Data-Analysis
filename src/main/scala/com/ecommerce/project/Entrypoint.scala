package com.ecommerce.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring, to_timestamp}

object Entrypoint {
  def main(args: Array[String]): Unit = {
    // create spark session
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("eCommerceAnalysis")
        .getOrCreate()

    val bucketName = args.head

    // prepare spark dataframe
    val filenames = Array("2019-Oct.csv", "2019-Nov.csv", "2019-Dec.csv",
      "2020-Jan.csv", "2020-Feb.csv", "2020-Mar.csv", "2020-Apr.csv")
    val dfs = filenames.map(f => spark
                              .read
                              .options(Map("header" -> "true", "inferSchema" -> "true")).csv("gs://%s/data/%s".format(bucketName, f)))
    val df = dfs.reduce(_.union(_))
      .withColumn("event_time", substring(col("event_time"), 0, 19))
      .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")).persist()

    // exploratory analysis
    val analyzer = ExploratoryAnalyzer(spark, df, bucketName)

    analyzer.exploreEvents()
    analyzer.explorePurchases()

    spark.stop()
  }
}
