package upm.bd

import org.apache.spark.sql.{DataFrame, SparkSession}

// It's a trait because it has not independent meaning, we use this trait to
// add functionality in an object-oriented manner.
object SparkSessionWrapper {

  // This variable should be accessed only within the object itself, i.e.,
  // using the 'this' keyword.
  lazy val spark: SparkSession =
  SparkSession
    .builder()
    .appName("SparkML")
    .master("local[*]")
    .getOrCreate()

}
