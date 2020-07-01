package com.ecommerce.project

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import com.google.common.base.CaseFormat
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ExploratoryAnalyzer(spark: SparkSession, df: DataFrame, bucketName: String) {
  df.createOrReplaceTempView("ecommerce")

  private val event_types = Array("view", "cart", "remove_from_cart", "purchase", "event")

  private def toCamel(s: String): String = {
    CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, s)
  }

  private def saveHistToFile(hist: (Array[Double], Array[Long]), filename: String): Unit = {
    val conf = new Configuration()
    val path = new Path(filename)
    val gcsFs = path.getFileSystem(conf)
    val outputStream = gcsFs.create(path)
    outputStream.writeChars(hist._1.mkString(",") + "\n")
    outputStream.writeChars(hist._2.mkString(",") + "\n")
    outputStream.close()
  }

  /*
  Explore everything about events happened
   */
  def exploreEvents(): Unit = {
    exploreTotalEvents(df)
    exploreUsersWithMaxEvents(df)
    exploreMostActiveUsers(df)
  }

  /*
  Explore everything about purchases & purchased items
   */
  def explorePurchases(): Unit = {
    exploreTurnover(df)
    exploreUsersWithMaxSpending(df)
    exploreMostGenerousUsers(df)
    explorePopularProducts(df)
  }

  /*
  Total number of views, cart, remove_from_cart, purchase, all events of all users per day & per month & over the period
   */
  private def exploreTotalEvents(df: DataFrame): Unit = {
    val functionName = "exploreTotalEvents"
    val dailySumStatements = event_types
      .take(4)
      .map(e => "SUM(CASE WHEN event_type = '%s' THEN 1 ELSE 0 END) AS num_%ss,\n".format(e, e))
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
    dailyDf.persist()

    dailyDf.write.csv("gs://%s/%s/eventsAggByDay.csv".format(bucketName, functionName))

    def computeAndSave(fieldIndex: Int, filename: String): Unit = {
      val bucketCount = 20
      val columnData = dailyDf.rdd.map(_.getLong(fieldIndex)).persist()
      var maxNumEvents = columnData.max()
      maxNumEvents = if (maxNumEvents > 0) maxNumEvents else 1
      var avgBinWidth = maxNumEvents / bucketCount
      avgBinWidth = if (avgBinWidth > 0) avgBinWidth else 1
      val buckets = ((0.asInstanceOf[Long] until maxNumEvents by avgBinWidth) :+ maxNumEvents)
        .map(_.asInstanceOf[Double])
        .toArray
      val totalHist = columnData.histogram(buckets)
      saveHistToFile((buckets, totalHist), filename)
    }

    event_types.indices.foreach(
      i => computeAndSave(i, "gs://%s/%s/total%ssHist.txt".format(bucketName, functionName, toCamel(event_types(i))))
    )

    dailyDf.createOrReplaceTempView("agg_by_day")

    val monthlySumStatements = event_types.map(e => "SUM(num_%ss) AS num_%ss,\n".format(e, e))
    val monthlySql =
      """
        |SELECT
        |	%s
        |	DATE_FORMAT(date, 'yyyy-MM') AS month
        |FROM
        |	agg_by_day
        |GROUP BY
        |	DATE_FORMAT(date, 'yyyy-MM')
    """.stripMargin.format(monthlySumStatements.mkString(""))
    val monthlyDf = spark.sql(monthlySql)
    monthlyDf.persist()

    monthlyDf.write.csv("gs://%s/%s/eventsAggByMonth.csv".format(bucketName, functionName))

    monthlyDf.createOrReplaceTempView("agg_by_month")

    val totalSumStatements = event_types.take(4).map(e => "SUM(num_%ss) AS num_%ss,\n".format(e, e)) :+
      "SUM(num_%ss) AS num_%ss\n".format(event_types(4), event_types(4))
    val totalSql =
      """
        |SELECT
        |	%s
        |FROM
        |	agg_by_month
      """.stripMargin.format(totalSumStatements.mkString(""))
    val totalDf = spark.sql(totalSql)
    totalDf.write.csv("gs://%s/%s/eventsAgg.csv".format(bucketName, functionName))
  }

  /*
  User with maximum number of views, cart, remove_from_cart, purchase, all events of each day & each month
   */
  private def exploreUsersWithMaxEvents(df: DataFrame): Unit = {
    val functionName = "exploreUsersWithMaxEvents"
    val dailySumStatements = event_types
      .take(4)
      .map(e => "SUM(CASE WHEN event_type = '%s' THEN 1 ELSE 0 END) AS num_%ss,\n".format(e, e))
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
    dailyTempViewDf.persist()
    dailyTempViewDf.createOrReplaceTempView("daily_events")

    val monthlySumStatements = event_types.map(e => "SUM(num_%ss) AS num_%ss,\n".format(e, e))
    val sqlMonthlyTempView =
      """
        |SELECT
        |	%s
        | user_id,
        |	DATE_FORMAT(date, 'yyyy-MM') AS month
        |FROM
        |	daily_events
        |GROUP BY
        | user_id,
        |	DATE_FORMAT(date, 'yyyy-MM')
    """.stripMargin.format(monthlySumStatements.mkString(""))
    val monthlyTempViewDf = spark.sql(sqlMonthlyTempView)
    monthlyTempViewDf.persist()
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
      resultDf.write.csv(filename)
    }

    event_types.foreach(e => computeAndSave(
      "num_%ss".format(e),
      "gs://%s/%s/topUser%sPerDayResult.csv".format(bucketName, functionName, toCamel(e)), true)
    )
    event_types.foreach(e => computeAndSave(
      "num_%ss".format(e),
      "gs://%s/%s/topUser%sPerMonthResult.csv".format(bucketName, functionName, toCamel(e)), false)
    )

  }

  /*
  Users with top 10 numbers of views, cart, remove_from_cart, purchase, all events over the period
   */
  private def exploreMostActiveUsers(df: DataFrame): Unit = {
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
      resultDf.write.csv(filename)
    }

    event_types.take(4).foreach(e => computeAndSave(
      "num_%ss".format(e),
      "'%s'".format(e),
      "gs://%s/%s/topUser%sResult.csv".format(bucketName, functionName, toCamel(e)))
    )
    computeAndSave(
      "num_events",
      "event_type",
      "gs://%s/%s/topUserEventResult.csv".format(bucketName, functionName)
    )
  }

  /*
  Explore daily, monthly & total turnovers
   */
  private def exploreTurnover(df: DataFrame): Unit = {
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
    dailyTurnoverDf.persist()

    dailyTurnoverDf.write.csv("gs://%s/%s/turnoverAggByDay.csv".format(bucketName, functionName))
    val turnoverHist = dailyTurnoverDf.rdd.map(_.getDouble(0)).histogram(20)
    saveHistToFile(turnoverHist, "gs://%s/%s/turnoverPerDayHist.txt".format(bucketName, functionName))

    dailyTurnoverDf.createOrReplaceTempView("agg_by_day")

    val monthlySql =
      """
        |SELECT
        |	SUM(turnover) AS turnover,
        |	DATE_FORMAT(date, 'yyyy-MM') AS month
        |FROM
        |	agg_by_day
        |GROUP BY
        |	DATE_FORMAT(date, 'yyyy-MM')
      """.stripMargin
    val monthlyTurnoverDf = spark.sql(monthlySql)
    monthlyTurnoverDf.persist()

    monthlyTurnoverDf.write.csv("gs://%s/%s/turnoverAggByMonth.csv".format(bucketName, functionName))

    monthlyTurnoverDf.createOrReplaceTempView("agg_by_month")

    val totalTurnoverSql =
      """
        |SELECT
        |	SUM(turnover) AS turnover
        |FROM
        |	agg_by_month
      """.stripMargin
    val totalTurnoverDf = spark.sql(totalTurnoverSql)
    totalTurnoverDf.write.csv("gs://%s/%s/turnoverAgg.csv".format(bucketName, functionName))
  }

  /*
  Discover users with highest daily & monthly spending on this ecommerce site
   */
  private def exploreUsersWithMaxSpending(df: DataFrame): Unit = {
    val functionName = "exploreUsersWithMaxSpending"
    val sqlDailyTempView =
      """
        |SELECT
        | SUM(price) AS spending,
        | user_id,
        |	DATE(event_time) AS date
        |FROM
        |	ecommerce
        |WHERE
        |	event_type = 'purchase'
        |GROUP BY
        | user_id,
        |	DATE(event_time)
    """.stripMargin
    val dailyTempViewDf = spark.sql(sqlDailyTempView)
    dailyTempViewDf.persist()
    dailyTempViewDf.createOrReplaceTempView("daily_spending")

    val sqlMonthlyTempView =
      """
        |SELECT
        |	SUM(spending) AS spending,
        | user_id,
        |	DATE_FORMAT(date, 'yyyy-MM') AS month
        |FROM
        |	daily_spending
        |GROUP BY
        | user_id,
        |	DATE_FORMAT(date, 'yyyy-MM')
    """.stripMargin
    val monthlyTempViewDf = spark.sql(sqlMonthlyTempView)
    monthlyTempViewDf.createOrReplaceTempView("monthly_spending")

    val sqlTemplate =
      """
        |SELECT
        |	user_id,
        | spending,
        |	%s
        |FROM
        |	(
        |	SELECT
        |		user_id, spending, %s, RANK() OVER(PARTITION BY %s
        |	ORDER BY
        |		spending DESC) rank
        |	FROM
        |		%s) AS foo
        |WHERE
        |	rank = 1
    """.stripMargin

    def computeAndSave(filename: String, isDaily: Boolean): Unit = {
      val sql =
        if (isDaily) sqlTemplate.format("date", "date", "date", "daily_spending")
        else sqlTemplate.format("month", "month", "month", "monthly_spending")
      val resultDf = spark.sql(sql)
      resultDf.write.csv(filename)
    }

    computeAndSave("gs://%s/%s/topUserSpendingPerDayResult.csv".format(bucketName, functionName), true)
    computeAndSave("gs://%s/%s/topUserSpendingPerMonthResult.csv".format(bucketName, functionName), false)
  }

  /*
  Users with top 10 spending over the period
   */
  private def exploreMostGenerousUsers(df: DataFrame): Unit = {
    val functionName = "exploreMostGenerousUsers"
    val sql =
      """
        |SELECT
        |	SUM(price) AS spending,
        |	user_id
        |FROM
        |	ecommerce
        |WHERE
        |	event_type = 'purchase'
        |GROUP BY
        |	user_id
        |ORDER BY
        |	spending DESC
        |LIMIT 10
    """.stripMargin

    val resultDf = spark.sql(sql)
    resultDf.write.csv("gs://%s/%s/topUserSpendingResult.csv".format(bucketName, functionName))
  }

  /*
  Determine the mostly purchased popular products (top 50) & brands (top 20) & categories (top 10) per month & over the period
   */
  private def explorePopularProducts(df: DataFrame): Unit = {
    val functionName = "explorePopularProducts"
    val columns = Array("product_id", "brand", "category_code")
    val topNums = Array(50, 20, 10)
    val checkNullSql =
      """
        |SELECT
        |	COUNT(*)
        |FROM
        |	ecommerce
        |WHERE
        |	%s IS NULL
      """.stripMargin
    val numsOfNulls = columns.map(c => spark.sql(checkNullSql.format(c)).rdd.collect().head.getLong(0))

    val sqlMonthlyTempView =
      """
        |SELECT
        |	COUNT(product_id) AS purchase_count,
        |	%s,
        |	DATE_FORMAT(event_time, 'yyyy-MM') AS MONTH
        |FROM
        |	ecommerce
        |WHERE
        |	event_type = 'purchase'
        |	AND %s IS NOT NULL
        |GROUP BY
        | %s,
        |	DATE_FORMAT(event_time, 'yyyy-MM')
    """.stripMargin
    val monthlyTempViewDfs = columns.map(c => spark.sql(sqlMonthlyTempView.format(c, c, c)).persist())
    val monthlyTempViewNames = columns.map(c => "monthly_%s".format(c))
    monthlyTempViewDfs.indices.foreach(i => monthlyTempViewDfs(i).createOrReplaceTempView(monthlyTempViewNames(i)))

    val sqlMonthly =
      """
        |SELECT
        | purchase_count,
        | %s,
        | rank,
        |	month
        |FROM
        |	(
        |	SELECT
        |		purchase_count, %s, month, RANK() OVER(PARTITION BY month
        |	ORDER BY
        |		purchase_count DESC) rank
        |	FROM
        |		%s) AS foo
        |WHERE
        |	rank <= %d
    """.stripMargin
    val monthlyDfs = columns.indices.map(i => spark
      .sql(sqlMonthly.format(columns(i), columns(i), monthlyTempViewNames(i), topNums(i))))
    monthlyDfs.indices.foreach(i => monthlyDfs(i)

      .write.csv("gs://%s/%s/mostPopular%sByMonth.csv".format(bucketName, functionName, toCamel(columns(i)))))

    val sqlTotal =
      """
        |SELECT
        |	SUM(purchase_count) AS purchase_count,
        |	%s
        |FROM
        |	%s
        |GROUP BY
        |	%s
        |ORDER BY
        | purchase_count DESC
        |LIMIT %d
      """.stripMargin

    val totalDfs = columns
      .indices
      .map(i => spark.sql(sqlTotal.format(columns(i), monthlyTempViewNames(i), columns(i), topNums(i))))
    totalDfs.indices.foreach(i => totalDfs(i)

      .write.csv("gs://%s/%s/mostPopular%s_%d.csv".format(bucketName, functionName, toCamel(columns(i)), numsOfNulls(i))))
  }
}
