import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, substring, to_timestamp}

object Analysis {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("eCommerceAnalysis")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    val df: DataFrame = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("data/2019-Dec.csv")
      .withColumn("event_time", substring(col("event_time"), 0, 19))
      .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))

    df.createOrReplaceTempView("ecommerce")

    exploreTotalEventsEachDay(df)
    exploreUsersWithMaxEvents(df)
    exploreMostActiveUsers(df)
    exploreTurnoverPerDay(df)
  }

  def saveHistToFile(hist: (Array[Double], Array[Long]), filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(hist._1.mkString(",") + "\n")
    bw.write(hist._2.mkString(",") + "\n")
    bw.close()
  }

  /*
  Total number of views, cart, remove_from_cart, purchase, all events of all users per day
   */
  def exploreTotalEventsEachDay(df: DataFrame): Unit = {
    val sql =
      """
        |SELECT
        |	SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS num_views,
        |	SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS num_carts,
        |	SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS num_remove_from_carts,
        |	SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS num_purchases,
        | COUNT(event_type) AS num_events,
        |	DATE(event_time) AS date
        |FROM
        |	ecommerce e
        |GROUP BY
        |	DATE(event_time)
  """.stripMargin
    val totalDf = spark.sql(sql)

    totalDf.coalesce(1).write.csv("exploreTotalEventsEachDay/eventsAggByDay.csv")

    def computeAndSave(fieldIndex: Int, filename: String): Unit = {
      val bucketCount = 50
      val columnData = totalDf.rdd.map(_.getLong(fieldIndex)).persist()
      val maxNumEvents = columnData.max()
      val avgBinWidth = maxNumEvents / bucketCount
      val buckets = ((0.asInstanceOf[Long] until maxNumEvents by avgBinWidth) :+ maxNumEvents)
        .map(_.asInstanceOf[Double])
        .toArray
      val totalHist = columnData.histogram(buckets)
      saveHistToFile((buckets, totalHist), filename)
    }

    computeAndSave(0, "exploreTotalEventsEachDay/totalViewsHist.txt")
    computeAndSave(1, "exploreTotalEventsEachDay/totalCartsHist.txt")
    computeAndSave(2, "exploreTotalEventsEachDay/totalRemoveFromCartsHist.txt")
    computeAndSave(3, "exploreTotalEventsEachDay/totalPurchasesHist.txt")
    computeAndSave(4, "exploreTotalEventsEachDay/totalEventsHist.txt")
  }

  /*
  User with maximum number of views, cart, remove_from_cart, purchase, all events of each day
   */
  def exploreUsersWithMaxEvents(df: DataFrame): Unit = {
    val sqlTempView =
      """
        |SELECT
        |	SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS num_views,
        |	SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS num_carts,
        |	SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS num_remove_from_carts,
        |	SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS num_purchases,
        | COUNT(event_type) AS num_events,
        | user_id,
        |	DATE(event_time) AS date
        |FROM
        |	ecommerce
        |GROUP BY
        | user_id,
        |	DATE(event_time)
  """.stripMargin
    val tempViewDf = spark.sql(sqlTempView)
    tempViewDf.createOrReplaceTempView("daily_events")

    val sqlTemplate =
      """
        |SELECT
        |	user_id,
        |	date
        |FROM
        |	(
        |	SELECT
        |		user_id, date, RANK() OVER(PARTITION BY date
        |	ORDER BY
        |		%s DESC) rank
        |	FROM
        |		daily_events) AS foo
        |WHERE
        |	rank = 1
    """.stripMargin

    def computeAndSave(fieldName: String, filename: String): Unit = {
      val sql = sqlTemplate.format(fieldName)
      val resultDf = spark.sql(sql)
      resultDf.coalesce(1).write.csv(filename)
    }

    computeAndSave("num_views", "exploreUsersWithMaxEvents/topUserViewPerDayResult.csv")
    computeAndSave("num_carts", "exploreUsersWithMaxEvents/topUserCartPerDayResult.csv")
    computeAndSave("num_remove_from_carts", "exploreUsersWithMaxEvents/topUserRemoveFromCartPerDayResult.csv")
    computeAndSave("num_purchases", "exploreUsersWithMaxEvents/topUserPurchasePerDayResult.csv")
    computeAndSave("num_events", "exploreUsersWithMaxEvents/topUserEventPerDayResult.csv")

  }

  /*
  Users with top 5 numbers of views, cart, remove_from_cart, purchase, all events over the period
   */
  def exploreMostActiveUsers(df: DataFrame): Unit = {
    val sqlTemplate =
      """
        |SELECT
        |	SUM(CASE WHEN event_type = %s THEN 1 ELSE 0 END) AS %s,
        |	user_id
        |FROM
        |	ecommerce
        |GROUP BY
        |	user_id
        |ORDER BY
        |	%s DESC
        |LIMIT 10
    """.stripMargin

    def computeAndSave(fieldName: String, fieldValue: String, filename: String): Unit = {
      val sql = sqlTemplate.format(fieldValue, fieldName, fieldName)
      val resultDf = spark.sql(sql)
      resultDf.coalesce(1).write.csv(filename)
    }

    computeAndSave("num_views", "'view'", "exploreMostActiveUsers/topUserViewResult.csv")
    computeAndSave("num_carts", "'cart'", "exploreMostActiveUsers/topUserCartResult.csv")
    computeAndSave("num_remove_from_carts", "'remove_from_cart'", "exploreMostActiveUsers/topUserRemoveFromCartResult.csv")
    computeAndSave("num_purchases", "'purchase'", "exploreMostActiveUsers/topUserPurchaseResult.csv")
    computeAndSave("num_events", "event_type", "exploreMostActiveUsers/topUserEventResult.csv")
  }

  def exploreTurnoverPerDay(df: DataFrame): Unit = {
    val sql =
      """
        |SELECT
        |	SUM(price) AS turnover,
        |	DATE(event_time) AS date
        |FROM
        |	ecommerce
        |WHERE
        |	event_type = 'purchase'
        |GROUP BY
        |	DATE(event_time)
    """.stripMargin
    val turnoverDf = spark.sql(sql)

    turnoverDf.coalesce(1).write.csv("exploreTurnoverPerDay/turnoverAggByDay.csv")
    val turnoverHist = turnoverDf.rdd.map(_.getDouble(0)).histogram(50)
    saveHistToFile(turnoverHist, "exploreTurnoverPerDay/turnoverPerDayHist.txt")
  }

}