package fdiazgon.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("sparkml-flights-delay")
      .master("local[*]")
      .getOrCreate()

}
