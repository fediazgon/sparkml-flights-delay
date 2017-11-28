package upm.bd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * print some interesting statistics on the data, mainly for fun
  */
class Explorator {
  import SparkSessionWrapper.spark.implicits._

  def explore( dataFrame : DataFrame ) : Unit =
  {
    //count the number of flights for carrier
    val groupedCarrier = dataFrame.groupBy($"UniqueCarrier")
    val flightPerCarrier = groupedCarrier.count().sort($"count".desc).select($"UniqueCarrier", $"count".as("flights"))
    groupedCarrier.agg(sum($"ArrDelay")).show(10)

  }

}
