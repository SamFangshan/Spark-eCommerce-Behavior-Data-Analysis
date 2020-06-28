import java.io.{BufferedWriter, File, FileWriter}

import com.google.common.base.CaseFormat
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

  val event_types = Array("view", "cart", "remove_from_cart", "purchase", "event")

  def main(args: Array[String]): Unit = {
    val df: DataFrame = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("data/2019-Dec.csv")
      .withColumn("event_time", substring(col("event_time"), 0, 19))
      .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))

    df.createOrReplaceTempView("ecommerce")

    exploreTotalEvents(df)
    exploreUsersWithMaxEvents(df)
    exploreMostActiveUsers(df)
    exploreTurnover(df)
  }

  def toCamel(s: String): String = {
    CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, s)
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
    val functionName = "exploreTotalEvents"
    val dailySumStatements = event_types.take(4).map(e => "SUM(CASE WHEN event_type = '%s' THEN 1 ELSE 0 END) AS num_%ss,\n".format(e, e))
    val dailySql =
      """
        |SELECT
        |	%s
        | COUNT(event_type) AS num_%ss,
        |	DATE(event_time) AS date
        |FROM
        |	ecommerce
        |GROUP BY
        |	DATE(event_time)
  """.stripMargin.format(dailySumStatements.mkString(""), event_types(4))
    val dailyDf = spark.sql(dailySql)

    dailyDf.coalesce(1).write.csv("%s/eventsAggByDay.csv".format(functionName))

    def computeAndSave(fieldIndex: Int, filename: String): Unit = {
      val bucketCount = 20
      val columnData = dailyDf.rdd.map(_.getLong(fieldIndex)).persist()
      val maxNumEvents = columnData.max()
      val avgBinWidth = maxNumEvents / bucketCount
      val buckets = ((0.asInstanceOf[Long] until maxNumEvents by avgBinWidth) :+ maxNumEvents)
        .map(_.asInstanceOf[Double])
        .toArray
      val totalHist = columnData.histogram(buckets)
      saveHistToFile((buckets, totalHist), filename)
    }

    event_types.indices.foreach(i => computeAndSave(i, "%s/total%ssHist.txt".format(functionName, toCamel(event_types(i)))))

    dailyDf.createOrReplaceTempView("agg_by_day")

    val monthlySumStatements = event_types.map(e => "SUM(num_%ss) AS num_%ss,\n".format(e, e))
    val monthlySql =
      """
        |SELECT
        |	%s
        |	EXTRACT(MONTH FROM date) AS month
        |FROM
        |	agg_by_day
        |GROUP BY
        |	EXTRACT(MONTH FROM date)
    """.stripMargin.format(monthlySumStatements.mkString(""))
    val monthlyTurnoverDf = spark.sql(monthlySql)
    monthlyTurnoverDf.coalesce(1).write.csv("%s/eventsAggByMonth.csv".format(functionName))
  }

  /*
  User with maximum number of views, cart, remove_from_cart, purchase, all events of each day & each month
   */
  def exploreUsersWithMaxEvents(df: DataFrame): Unit = {
    val functionName = "exploreUsersWithMaxEvents"
    val dailySumStatements = event_types.take(4).map(e => "SUM(CASE WHEN event_type = '%s' THEN 1 ELSE 0 END) AS num_%ss,\n".format(e, e))
    val sqlDailyTempView =
      """
        |SELECT
        |	%s
        | COUNT(event_type) AS num_%ss,
        | user_id,
        |	DATE(event_time) AS date
        |FROM
        |	ecommerce
        |GROUP BY
        | user_id,
        |	DATE(event_time)
    """.stripMargin.format(dailySumStatements.mkString(""), event_types(4))
    val dailyTempViewDf = spark.sql(sqlDailyTempView)
    dailyTempViewDf.createOrReplaceTempView("daily_events")

    val monthlySumStatements = event_types.map(e => "SUM(num_%ss) AS num_%ss,\n".format(e, e))
    val sqlMonthlyTempView =
      """
        |SELECT
        |	%s
        | user_id,
        |	EXTRACT(MONTH FROM date) AS month
        |FROM
        |	daily_events
        |GROUP BY
        | user_id,
        |	EXTRACT(MONTH FROM date)
    """.stripMargin.format(monthlySumStatements.mkString(""))
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

    event_types.foreach(e => computeAndSave("num_%ss".format(e), "%s/topUser%sPerDayResult.csv".format(functionName, toCamel(e)), true))
    event_types.foreach(e => computeAndSave("num_%ss".format(e), "%s/topUser%sPerMonthResult.csv".format(functionName, toCamel(e)), false))

  }

  /*
  Users with top 5 numbers of views, cart, remove_from_cart, purchase, all events over the period
   */
  def exploreMostActiveUsers(df: DataFrame): Unit = {
    val functionName = "exploreMostActiveUsers"
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

    event_types.take(4).foreach(e => computeAndSave("num_%ss".format(e), "'%s'".format(e), "%s/topUser%sResult.csv".format(functionName, toCamel(e))))
    computeAndSave("num_events", "event_type", "%s/topUserEventResult.csv".format(functionName))
  }

  /*
  Explore daily and monthly turnovers
   */
  def exploreTurnover(df: DataFrame): Unit = {
    val functionName = "exploreTurnover"
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

    dailyTurnoverDf.coalesce(1).write.csv("%s/turnoverAggByDay.csv".format(functionName))
    val turnoverHist = dailyTurnoverDf.rdd.map(_.getDouble(0)).histogram(20)
    saveHistToFile(turnoverHist, "%s/turnoverPerDayHist.txt".format(functionName))

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
    monthlyTurnoverDf.coalesce(1).write.csv("%s/turnoverAggByMonth.csv".format(functionName))
  }

}