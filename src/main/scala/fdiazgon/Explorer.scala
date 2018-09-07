package fdiazgon

import fdiazgon.utils.{LoggingUtils, SparkSessionWrapper}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._


/**
  * Prints some interesting statistics on the data.
  */
class Explorer extends SparkSessionWrapper {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  import spark.implicits._

  def explore(data: Dataset[_]): Unit = {

    LoggingUtils.printHeader("EXPLORING")

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

    cancelledRatio(data)
  }

  private def cancelledRatio(data: Dataset[_]): Unit = {

    logger.info("Cancelled flights stats:")

    val cancelledCount =
      data
        .select(col("Cancelled"))
        .groupBy(col("Cancelled"))
        .count
        .rdd
        .collect() // We can call collect. It's going to have 2 rows only

    // TODO: this throws an exception when there is no cancelled flights
    val (cancelled, notCancelled) = (cancelledCount(0).getLong(1), cancelledCount(1).getLong(1))
    val total = cancelled + notCancelled

    logger.info(f"Over $total%d flights, ${cancelled * 1.0 / total * 100}%2.2f were cancelled")
    logger.info(s"Removing $cancelled cancelled flights")
  }

}
