package upm.bd

import org.apache.spark.sql.{DataFrame, SparkSession}

// It's a trait because it has not independent meaning, we use this trait to
// add functionality in an object-oriented manner.
trait SparkSessionWrapper {

  // This variable should be accessed only within the object itself, i.e.,
  // using the 'this' keyword.
  protected[this] lazy val spark: SparkSession =
  SparkSession
    .builder()
    .appName("SparkML")
    .master("local[*]")
    .getOrCreate()


  // Here we can do all the data pre-processing and then shared it among
  // all the classes extending this trait.
  protected def flightsData(dataFile: String): DataFrame = {

    val forbiddenVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn",
      "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
      "SecurityDelay", "LateAircraftDelay")

    spark.read
      .option("header", value = true)
      .csv(dataFile)
      .drop(forbiddenVariables: _*)
      .cache()

  }

}
