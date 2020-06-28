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

//    exploreTotalEvents(df)
    exploreUsersWithMaxEvents(df)
//    exploreMostActiveUsers(df)
//    exploreTurnover(df)
  }

  def saveHistToFile(hist: (Array[Double], Array[Long]), filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(hist._1.mkString(",") + "\n")
    bw.write(hist._2.mkString(",") + "\n")
    bw.close()
  }

  /*
  Total number of views, cart, remove_from_cart, purchase, all events of all users per day & per month
   */
  def exploreTotalEvents(df: DataFrame): Unit = {
    val dailySql =
      """
        |SELECT
        |	SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS num_views,
        |	SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS num_carts,
        |	SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS num_remove_from_carts,
        |	SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS num_purchases,
        | COUNT(event_type) AS num_events,
        |	DATE(event_time) AS date
        |FROM
        |	ecommerce
        |GROUP BY
        |	DATE(event_time)
  """.stripMargin
    val dailyDf = spark.sql(dailySql)

    dailyDf.coalesce(1).write.csv("exploreTotalEvents/eventsAggByDay.csv")

    def computeAndSave(fieldIndex: Int, filename: String): Unit = {
      val bucketCount = 50
      val columnData = dailyDf.rdd.map(_.getLong(fieldIndex)).persist()
      val maxNumEvents = columnData.max()
      val avgBinWidth = maxNumEvents / bucketCount
      val buckets = ((0.asInstanceOf[Long] until maxNumEvents by avgBinWidth) :+ maxNumEvents)
        .map(_.asInstanceOf[Double])
        .toArray
      val totalHist = columnData.histogram(buckets)
      saveHistToFile((buckets, totalHist), filename)
    }

    computeAndSave(0, "exploreTotalEvents/totalViewsHist.txt")
    computeAndSave(1, "exploreTotalEvents/totalCartsHist.txt")
    computeAndSave(2, "exploreTotalEvents/totalRemoveFromCartsHist.txt")
    computeAndSave(3, "exploreTotalEvents/totalPurchasesHist.txt")
    computeAndSave(4, "exploreTotalEvents/totalEventsHist.txt")

    dailyDf.createOrReplaceTempView("agg_by_day")

    val monthlySql =
      """
        |SELECT
        |	SUM(num_views) AS num_views,
        | SUM(num_carts) AS num_carts,
        | SUM(num_remove_from_carts) AS num_remove_from_carts,
        | SUM(num_purchases) AS num_purchases,
        | SUM(num_events) AS num_events,
        |	EXTRACT(MONTH FROM date) AS month
        |FROM
        |	agg_by_day
        |GROUP BY
        |	EXTRACT(MONTH FROM date)
    """.stripMargin
    val monthlyTurnoverDf = spark.sql(monthlySql)
    monthlyTurnoverDf.coalesce(1).write.csv("exploreTotalEvents/eventsAggByMonth.csv")
  }

  /*
  User with maximum number of views, cart, remove_from_cart, purchase, all events of each day & each month
   */
  def exploreUsersWithMaxEvents(df: DataFrame): Unit = {
    val sqlDailyTempView =
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
    val dailyTempViewDf = spark.sql(sqlDailyTempView)
    dailyTempViewDf.createOrReplaceTempView("daily_events")

    val sqlMonthlyTempView =
      """
        |SELECT
        |	SUM(num_views) AS num_views,
        | SUM(num_carts) AS num_carts,
        | SUM(num_remove_from_carts) AS num_remove_from_carts,
        | SUM(num_purchases) AS num_purchases,
        | SUM(num_events) AS num_events,
        | user_id,
        |	EXTRACT(MONTH FROM date) AS month
        |FROM
        |	daily_events
        |GROUP BY
        | user_id,
        |	EXTRACT(MONTH FROM date)
    """.stripMargin
    val monthlyTempViewDf = spark.sql(sqlMonthlyTempView)
    monthlyTempViewDf.createOrReplaceTempView("monthly_events")

    val sqlTemplate =
      """
        |SELECT
        |	user_id,
        | %s
        |	%s
        |FROM
        |	(
        |	SELECT
        |		user_id, %s, %s, RANK() OVER(PARTITION BY %s
        |	ORDER BY
        |		%s DESC) rank
        |	FROM
        |		%s) AS foo
        |WHERE
        |	rank = 1
    """.stripMargin

    def computeAndSave(fieldName: String, filename: String, isDaily: Boolean): Unit = {
      val sql =
        if (isDaily) sqlTemplate.format(fieldName, "date", fieldName, "date", "date", fieldName, "daily_events")
        else sqlTemplate.format(fieldName, "month", fieldName, "month", "month", fieldName, "monthly_events")
      val resultDf = spark.sql(sql)
      resultDf.coalesce(1).write.csv(filename)
    }

    computeAndSave("num_views", "exploreUsersWithMaxEvents/topUserViewPerDayResult.csv", true)
    computeAndSave("num_carts", "exploreUsersWithMaxEvents/topUserCartPerDayResult.csv", true)
    computeAndSave("num_remove_from_carts", "exploreUsersWithMaxEvents/topUserRemoveFromCartPerDayResult.csv", true)
    computeAndSave("num_purchases", "exploreUsersWithMaxEvents/topUserPurchasePerDayResult.csv", true)
    computeAndSave("num_events", "exploreUsersWithMaxEvents/topUserEventPerDayResult.csv", true)

    computeAndSave("num_views", "exploreUsersWithMaxEvents/topUserViewPerMonthResult.csv", false)
    computeAndSave("num_carts", "exploreUsersWithMaxEvents/topUserCartPerMonthResult.csv", false)
    computeAndSave("num_remove_from_carts", "exploreUsersWithMaxEvents/topUserRemoveFromCartPerMonthResult.csv", false)
    computeAndSave("num_purchases", "exploreUsersWithMaxEvents/topUserPurchasePerMonthResult.csv", false)
    computeAndSave("num_events", "exploreUsersWithMaxEvents/topUserEventPerMonthResult.csv", false)

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

  /*
  Explore daily and monthly turnovers
   */
  def exploreTurnover(df: DataFrame): Unit = {
    val dailySql =
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
    val dailyTurnoverDf = spark.sql(dailySql)

    dailyTurnoverDf.coalesce(1).write.csv("exploreTurnover/turnoverAggByDay.csv")
    val turnoverHist = dailyTurnoverDf.rdd.map(_.getDouble(0)).histogram(50)
    saveHistToFile(turnoverHist, "exploreTurnover/turnoverPerDayHist.txt")

    dailyTurnoverDf.createOrReplaceTempView("agg_by_day")

    val monthlySql =
      """
        |SELECT
        |	SUM(turnover) AS turnover,
        |	EXTRACT(MONTH FROM date) AS month
        |FROM
        |	agg_by_day
        |GROUP BY
        |	EXTRACT(MONTH FROM date)
      """.stripMargin
    val monthlyTurnoverDf = spark.sql(monthlySql)
    monthlyTurnoverDf.coalesce(1).write.csv("exploreTurnover/turnoverAggByMonth.csv")
  }

}