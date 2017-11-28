package upm.bd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.sql.functions._

class Preprocesser( delayThreshold : Int = 15 ,verbose: Boolean = true) {

  import SparkSessionWrapper.spark.implicits._

  def preprocess(filePath: String): DataFrame = {

    var df = SparkSessionWrapper.spark //using it as var because we are going to do casting and dropping
      .read
      .option("header", true)
      .csv(filePath)
      .withColumn("DayOfWeek", $"DayOfWeek".cast("int"))
      .withColumn("DepDelay", $"DepDelay".cast("int")) // compact column syntax
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int")) //keeping it as an int to exploit aggregation functions
      .withColumn("Diverted", $"Diverted".cast("int"))
      .withColumn("TaxiOut", $"TaxiOut".cast("int")) //we can use it because we havent still took off
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))
      .withColumn("Year",$"Year".cast("int"))
      .withColumn("Month",$"Month".cast("int"))
      .withColumn("DayofMonth",$"DayofMonth".cast("int"))


    //remove diverted flights because they hanve null ArrDelay
    df = df.filter($"Diverted" === 0)

    val forbiddenVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay",
      "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
    df = df.drop(forbiddenVariables: _*) //like args builtin in python

    showDf(df)

    //remove cancelled flights
    df = removeCancelled(df)
    df = df.drop("Cancelled", "CancellationCode")

    //lets create a column for weekends
    val isWeekEndUdf = udf(
      (dayOfWeek : Int) => if (dayOfWeek == 6 || dayOfWeek == 7) 1 else 0
    )

    val hourExtractorUdf = udf(
      (timeString : String) =>
        {
          if (timeString.length > 2) timeString.substring(0,timeString.length - 2).toInt else 0
        }

    )
    //create a column if the delay is more than the threshould, maybe we will make a binary classifier
    //I am using the SQL api because i got a non serializable exception if I use the delayThreshold as a value
    df = df.select($"*",($"ArrDelay" > lit(delayThreshold)).as("OverDelay"))
    df = df.withColumn("WeekEnd", isWeekEndUdf($"DayofWeek"))
      .withColumn("CRSDepHour", hourExtractorUdf($"CRSDepTime"))
      .withColumn("DepHour", hourExtractorUdf($"DepTime"))


    //adding the route row, can be interesting
    df = df.select($"*",(concat($"Origin",lit("-"),$"Dest")).as("Route"))

    //we drop columns we do not think to be worth it
    df = df.drop("CRSDepTime", "CRSArrTime", "DepTime", "FlightNum", "TailNum")






    if(df.filter($"ArrDelay".isNull).count() > 0)
    {
      println("WARN: There are records with null target variable after preprocessing")
    }

    showDf(df)
    if (verbose) df.printSchema()


    df
  }

  private def removeCancelled(dataFrame: DataFrame): DataFrame = {
    //remove cancelled rows
    if (verbose) //some stats on cancelled
    {
      val cancelledCount = dataFrame.select(col("Cancelled"))
        .groupBy(col("Cancelled"))
        .count()
        .rdd.collect()

      val (cancelled, notCancelled) = (cancelledCount(0).getLong(1), cancelledCount(1).getLong(1))
      val total = cancelled + notCancelled

      println(f"Over $total%d flights, ${cancelled * 1.0 / total * 100}%2.2f%% were cancelled")
      println(s"Removing $cancelled cancelled flights")
    }
    dataFrame.filter($"Cancelled" === 0)
  }


  private def showDf(dataFrame: DataFrame, lines: Int = 10): Unit = {
    if (verbose) dataFrame.show(lines)
  }


}
