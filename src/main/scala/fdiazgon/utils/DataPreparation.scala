package fdiazgon.utils

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._


/**
  * Different steps of the machine learning process.
  */
object DataPreparation extends SparkSessionWrapper {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  import spark.implicits._

  /**
    * Print some interesting statistics of the data.
    *
    * @param data dataset to explore.
    */
  def explore(data: Dataset[_]): Unit = {

    LoggingUtils.printHeader("Exploring")

    val groupedCarrier = data.groupBy($"UniqueCarrier")

    logger.info("Flights by carrier:")
    groupedCarrier
      .count()
      .sort($"count".desc)
      .select($"UniqueCarrier", $"count".as("flights"))
      .show(10)

    logger.info("Total delay by carrier:")
    groupedCarrier
      .agg(sum($"ArrDelay"))
      .show(10)

    cancelledRatioStats(data)
  }

  private def cancelledRatioStats(data: Dataset[_]): Unit = {

    logger.info("Cancelled flights stats:")

    val cancelledCount =
      data
        .select(col("Cancelled"))
        .groupBy(col("Cancelled"))
        .count
        .rdd
        .collect()

    // TODO: this throws an exception when there is no cancelled flights
    val (cancelled, notCancelled) = (cancelledCount(0).getLong(1), cancelledCount(1).getLong(1))
    val total = cancelled + notCancelled

    logger.info(f"Over $total%d flights, ${cancelled * 1.0 / total * 100}%2.2f were cancelled")
    logger.info(s"Removing $cancelled cancelled flights")
  }

  def preprocess(dataset: Dataset[_]): Dataset[_] = {

    LoggingUtils.printHeader("Preprocessing")

    val forbiddenVariables = Seq(
      "ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted",
      "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
    logger.info(s"Removing forbidden variables: ${forbiddenVariables.mkString(", ")}")
    var ds = dataset.drop(forbiddenVariables: _*)

    // Remove diverted flights because they have null ArrDelay
    logger.info("Removing diverted flights")
    ds = ds.filter($"Diverted" === 0)

    logger.info("Removing cancelled flights")
    ds = ds.filter($"Cancelled" === 0).drop("Cancelled", "CancellationCode")

    // Check null values in target column. They are not expected so I want to inspect
    val nullValuesDf = ds.filter($"ArrDelay".isNull)
    if (nullValuesDf.count() > 0) {
      logger.warn("We still have null values! Please check why! We already have removed the expected source of nulls.")
      nullValuesDf.show()
      logger.info("Removing remaining null values")
      ds = ds.filter($"ArrDelay".isNotNull)
    }
    else {
      logger.info("No null values in target column")
    }

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

    logger.info("Converting time columns")
    ds = ds
      .withColumn(Constants.NEW_COL_CRS_DEP_TIME_MIN, minutesConverter($"CRSDepTime"))
      .withColumn(Constants.NEW_COL_DEP_TIME_MIN, minutesConverter($"DepTime"))

    // Adding the route, can be interesting
    logger.info(s"Adding ${Constants.NEW_COL_ROUTE} column")
    ds = ds.select($"*", concat($"Origin", lit("-"), $"Dest").as(Constants.NEW_COL_ROUTE))

    // We drop columns that we do not think to be worth it
    val toDrop = Array("CRSDepTime", "DepTime", "CRSArrTime", "FlightNum", "TailNum")
    logger.info(s"Dropping non-worthy columns: ${toDrop.mkString(", ")}")
    ds = ds.drop(toDrop: _*)

    logger.info("Final DataFrame:")
    ds.show(15)

    ds
  }

}
