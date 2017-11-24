package upm.bd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Preprocesser(verbose: Boolean = true) {


  def preprocess(filePath : String) : DataFrame =
  {
    import SparkSessionWrapper.spark.implicits._
    var df = SparkSessionWrapper.spark//using it as var because we are going to do casting and dropping
      .read
      .option("header",true)
      .csv(filePath)
      .withColumn("DepDelay", $"DepDelay".cast("int"))// compact column syntax
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int")) //keeping it as an int to exploit aggregation functions
      .withColumn("Diverted",$"Diverted".cast("int"))
      .withColumn("TaxiOut",$"TaxiOut".cast("int")) //we can use it because we havent still took off

    val forbiddenVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay",
      "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
    df = df.drop(forbiddenVariables:_*) //like args builtin in python

    showDf(df)

    df
  }

  private def showDf(dataFrame: DataFrame, lines :Int = 10) : Unit =
  {
    if(verbose) dataFrame.show(lines)
  }


}
