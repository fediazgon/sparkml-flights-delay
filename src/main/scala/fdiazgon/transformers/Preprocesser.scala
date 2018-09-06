package fdiazgon.transformers

import fdiazgon.utils.SparkSessionWrapper.spark.implicits._
import fdiazgon.utils.{DataFrameUtils, LoggingUtils}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

class Preprocesser(delayThreshold: Int = 15) {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  def preprocess(dataset: Dataset[_]): DataFrame = {

    LoggingUtils.printHeader("PREPROCESSING")

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
    logger.info("Removing diverted flights")
    df = df.filter($"Diverted" === 0)

    logger.info("Removing cancelled flights")
    df = df.filter($"Cancelled" === 0).drop("Cancelled", "CancellationCode")

    // Check null values. They are not expected so I want to inspect
    val nullValuesDf = df.filter($"ArrDelay".isNull)
    if (nullValuesDf.count() > 0) {
      logger.warn("We still have null values! Please check why!\n" +
        "We already have removed the expected source of nulls.")
      DataFrameUtils.show(nullValuesDf)
      logger.info("Removing remaining null values")
      df = df.filter($"ArrDelay".isNotNull)
    }
    else {
      logger.info("No null values in target column")
    }

    val forbiddenVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn",
      "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
      "SecurityDelay", "LateAircraftDelay")
    logger.info(s"Removing forbidden variables: ${forbiddenVariables.mkString(", ")}")
    df = df.drop(forbiddenVariables: _*)

    // Convert in minutes since midnight
    val minutesConverter = udf(
      (timeString: String) => {
        val hours =
          if (timeString.length > 2)
            timeString.substring(0, timeString.length - 2).toInt
          else
            0
        hours * 60 + timeString.takeRight(2).toInt
      })

    // Create a column if the delay is more than the threshold
    // Maybe we will make a binary classifier. This is beyond the scope of the exam
    logger.info("Adding 'OverDelay' and 'WeekEnd' columns")
    df = df.select(
      $"*",
      ($"ArrDelay" > lit(delayThreshold)).as("OverDelay").cast("int"),
      (col("DayOfWeek") === 6 || col("DayOfWeek") === 7).as("WeekEnd")
    )

    logger.info("Converting time columns")
    df = df
      .withColumn("CRSDepTimeMin", minutesConverter($"CRSDepTime"))
      .withColumn("DepTimeMin", minutesConverter($"DepTime"))

    // Adding the route row, can be interesting
    logger.info("Adding 'Route' column")
    df = df.select($"*", concat($"Origin", lit("-"), $"Dest").as("Route"))

    // We drop columns we do not think to be worth it
    val toDrop = Array("CRSDepTime", "CRSArrTime", "DepTime", "FlightNum", "TailNum")
    logger.info(s"Dropping non-worthy columns: ${toDrop.mkString(", ")}")
    df = df.drop(toDrop: _*)

    logger.info("Final DataFrame:")
    DataFrameUtils.show(df)

    df
  }

}
