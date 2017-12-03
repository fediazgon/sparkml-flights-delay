package upm.bd.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import upm.bd.utils.SparkSessionWrapper.spark.implicits._
import upm.bd.utils.{DataFrameUtils, MyLogger}

class Preprocesser(delayThreshold: Int = 15, verbose: Boolean = true) extends Transformer {

  override def transform(dataset: Dataset[_]): DataFrame = {

    MyLogger.printHeader("PREPROCESSING")

    var df = dataset
      .withColumn("Year", $"Year".cast("int"))
      .withColumn("Month", $"Month".cast("int"))
      .withColumn("DayofMonth", $"DayofMonth".cast("int"))
      .withColumn("DayOfWeek", $"DayOfWeek".cast("int"))
      .withColumn("CRSElapsedTime", $"CRSElapsedTime".cast("int"))
      .withColumn("DepDelay", $"DepDelay".cast("int"))
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("TaxiOut", $"TaxiOut".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int"))
      .withColumn("Diverted", $"Diverted".cast("int"))
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))

    // Remove diverted flights because they have null ArrDelay
    df = df.filter($"Diverted" === 0)
    // TODO: Read this Giorgio. Maybe na.drop is a better approach
    // df = df.drop(forbiddenVariables: _*).na.drop(Seq("ArrDelay"))
    // The thing is that flights with null "ArrDelay" are flights that
    // have been cancelled or diverted. So, let's just remove all flights
    // without "ArrDelay".

    val forbiddenVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted",
      "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
    MyLogger.info(s"Removing forbidden variables")
    df = df.drop(forbiddenVariables: _*)

    if (verbose) {
      MyLogger.info("Resulting DataFrame:")
      DataFrameUtils.show(df)
    }

    MyLogger.info("Removing cancelled flights")
    df = removeCancelled(df)

    // Lets create a column for weekends
    val isWeekEndUdf = udf(
      (dayOfWeek: Int) => if (dayOfWeek == 6 || dayOfWeek == 7) 1 else 0
    )

    val hourExtractorUdf = udf(
      (timeString: String) => {
        if (timeString.length > 2) timeString.substring(0, timeString.length - 2).toInt else 0
      })

    // Create a column if the delay is more than the threshold, maybe we will make a binary classifier
    // I am using the SQL API because I got a non serializable exception if I use the delayThreshold as a value
    MyLogger.info("Adding 'OverDelay', 'OverDelay', 'CRSDepHour' and 'DepHour' columns")
    df = df.select($"*", ($"ArrDelay" > lit(delayThreshold)).as("OverDelay"))
    df = df.withColumn("WeekEnd", isWeekEndUdf($"DayofWeek"))
      .withColumn("CRSDepHour", hourExtractorUdf($"CRSDepTime"))
      .withColumn("DepHour", hourExtractorUdf($"DepTime"))

    // Adding the route row, can be interesting
    MyLogger.info("Adding 'Route' column")
    df = df.select($"*", concat($"Origin", lit("-"), $"Dest").as("Route"))

    // We drop columns we do not think to be worth it
    MyLogger.info("Dropping non-worthy columns: 'CRSDepTime', 'CRSArrTime', 'DepTime', 'DepTime' and 'TailNum'")
    df = df.drop("CRSDepTime", "CRSArrTime", "DepTime", "FlightNum", "TailNum")


    MyLogger.info("Final DataFrame:")
    DataFrameUtils.show(df)
    if (verbose)
      df.printSchema()

    df
  }

  private def removeCancelled(dataFrame: DataFrame): DataFrame = {
    // Remove cancelled rows
    if (verbose) // Some stats on cancelled
    {
      val cancelledCount =
        dataFrame
          .select(col("Cancelled"))
          .groupBy(col("Cancelled"))
          .count
          .rdd
          .collect() // We can call collect. It's going to have 2 rows only

      // TODO: this throws an exception when there is no cancelled flights
      val (cancelled, notCancelled) =
        (cancelledCount(0).getLong(1),
          cancelledCount(1).getLong(1))
      val total = cancelled + notCancelled

      MyLogger.info("Cancelled flights stats:")
      MyLogger.info(f"Over $total%d flights, ${cancelled * 1.0 / total * 100}%2.2f%% were cancelled")
      MyLogger.info(s"Removing $cancelled cancelled flights")
    }
    dataFrame.filter($"Cancelled" === 0).drop("Cancelled", "CancellationCode")
  }

  override def copy(extra: ParamMap): Preprocesser = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override val uid: String = Identifiable.randomUID(this.getClass.getName)
}
