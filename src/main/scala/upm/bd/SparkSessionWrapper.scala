package upm.bd

import org.apache.spark.sql.{DataFrame, SparkSession}

// It's a trait because it has not independent meaning, we use this trait to
// add functionality in an object-oriented manner.
trait SparkSessionWrapper {

  private val FILE_PATH: String = "raw/2008.csv.bz2"

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
  protected def flightsData: DataFrame = {

    val forbiddenVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn",
      "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
      "SecurityDelay", "LateAircraftDelay")

    spark.read
      .option("header", value = true)
      .csv(FILE_PATH)
      .drop(forbiddenVariables: _*)
      .cache()

  }

}
