package upm.bd

import org.rogach.scallop._
import upm.bd.pipelines._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val rawFilePath: ScallopOption[String] = trailArg[String](required = false)
  verify()
}

object FlightDelayApp {

  private val DEFAULT_FILE_PATH: String = "raw/2008.csv"

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val filePath = conf.rawFilePath.getOrElse(DEFAULT_FILE_PATH)

    val rawDf = CSVReader.read(filePath, hasHeader = true)

    // new LinearRegressionPipeline(rawDf).run()

    new RandomForestPipeline(rawDf).run()

    //    new ComparatorPipeline(rawDf).run()
    //
    //    new LinearRegressionTuningPipeline(rawDf).run()
    //
    //    new RandomForestTuningPipeline(rawDf).run()

  }
}