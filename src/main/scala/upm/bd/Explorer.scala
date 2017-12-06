package upm.bd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import upm.bd.utils.MyLogger


/**
  * Prints some interesting statistics on the data.
  */
class Explorer {

  import upm.bd.utils.SparkSessionWrapper.spark.implicits._

  def explore(dataFrame: DataFrame): Unit = {

    MyLogger.printHeader("EXPLORING")

    // Count the number of flights by carrier
    val groupedCarrier = dataFrame.groupBy($"UniqueCarrier")

    MyLogger.info("Flights by carrier:")
    groupedCarrier
      .count()
      .sort($"count".desc)
      .select($"UniqueCarrier", $"count".as("flights"))
      .show(10)

    MyLogger.info("Total delay by carrier:")
    groupedCarrier
      .agg(sum($"ArrDelay"))
      .show(10)

    cancelledRatio(dataFrame)

  }

  private def cancelledRatio(dataFrame: DataFrame): Unit = {

    MyLogger.info("Cancelled flights stats:")

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

    MyLogger.info(f"Over $total%d flights, " +
      f"${cancelled * 1.0 / total * 100}%2.2f%% were cancelled")
    MyLogger.info(s"Removing $cancelled cancelled flights")
  }

}
