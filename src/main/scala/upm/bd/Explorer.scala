package upm.bd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import upm.bd.utils.MyLogger


/**
  * Prints some interesting statistics on the data, mainly for fun
  */
class Explorer {

  import upm.bd.utils.SparkSessionWrapper.spark.implicits._

  def explore(dataFrame: DataFrame): Unit = {

    MyLogger.printHeader("EXPLORING")

    // Count the number of flights for carrier
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
  }

}
