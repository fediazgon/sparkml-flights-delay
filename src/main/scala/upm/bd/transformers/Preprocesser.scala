package upm.bd.transformers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import upm.bd.utils.SparkSessionWrapper.spark.implicits._
import upm.bd.utils.{DataFrameUtils, MyLogger}

class Preprocesser(delayThreshold: Int = 15) {

  def preprocess(dataset: Dataset[_]): DataFrame = {

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
    MyLogger.info("Removing diverted flights")
    df = df.filter($"Diverted" === 0)

    MyLogger.info("Removing cancelled flights")
    df = df.filter($"Cancelled" === 0).drop("Cancelled", "CancellationCode")

    // Check null values. They are not expected so I want to inspect
    val nullValuesDf = df.filter($"ArrDelay".isNull)
    if (nullValuesDf.count() > 0) {
      MyLogger.warn("We still have null values! Please check why!\n" +
        "We already have removed the expected source of nulls.")
      DataFrameUtils.show(nullValuesDf)
      MyLogger.info("Removing remaining null values")
      df = df.filter($"ArrDelay".isNotNull)
    }
    else {
      MyLogger.info("No null values in target column")
    }

    val forbiddenVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn",
      "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
      "SecurityDelay", "LateAircraftDelay")
    MyLogger.info(s"Removing forbidden variables: ${forbiddenVariables.mkString(", ")}")
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
    MyLogger.info("Adding 'OverDelay' and 'WeekEnd' columns")
    df = df.select(
      $"*",
      ($"ArrDelay" > lit(delayThreshold)).as("OverDelay").cast("int"),
      (col("DayOfWeek") === 6 || col("DayOfWeek") === 7).as("WeekEnd")
    )

    MyLogger.info("Converting time columns")
    df = df
      .withColumn("CRSDepTimeMin", minutesConverter($"CRSDepTime"))
      .withColumn("DepTimeMin", minutesConverter($"DepTime"))

    // Adding the route row, can be interesting
    MyLogger.info("Adding 'Route' column")
    df = df.select($"*", concat($"Origin", lit("-"), $"Dest").as("Route"))

    // We drop columns we do not think to be worth it
    val toDrop = Array("CRSDepTime", "CRSArrTime", "DepTime", "FlightNum", "TailNum")
    MyLogger.info(s"Dropping non-worthy columns: ${toDrop.mkString(", ")}")
    df = df.drop(toDrop: _*)

    MyLogger.info("Final DataFrame:")
    DataFrameUtils.show(df)

    df
  }

}
