package upm.bd

import org.apache.log4j.{Level, Logger}

class LinearRegression(var dataFile: String) extends SparkSessionWrapper {

  def exec(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val df = flightsData(dataFile)
    df show 5
  }

}
