package upm.bd

import org.apache.log4j.{Level, Logger}

object LinearRegression extends SparkSessionWrapper {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val df = flightsData
    df show 5
  }

}
