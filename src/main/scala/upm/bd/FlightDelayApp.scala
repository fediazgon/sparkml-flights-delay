package upm.bd

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  //  val apples = opt[Int](required = true)
  //  val bananas = opt[Int]()
  val rawFilePath = trailArg[String](required = false)
  verify()
}

object FlightDelayApp {

  private val DEFAULT_FILE_PATH: String = "raw/2008.csv.bz2"

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    val conf = new Conf(args)
    val rawFilePath = conf.rawFilePath.getOrElse(DEFAULT_FILE_PATH)
    println(s"Using file $rawFilePath")

    val spark = SparkSessionWrapper.spark // Get it evaluated here

    val preprocesser = new Preprocesser
    val featuresCreator = new FeaturesCreator

    val preprocessedDf = preprocesser.preprocess(rawFilePath)
    preprocessedDf.show(10)

    featuresCreator.transform(preprocessedDf).show()

  }

}