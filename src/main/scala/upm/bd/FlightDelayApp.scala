package upm.bd

import org.rogach.scallop._
import upm.bd.pipelines.{LinearRegressionPipeline, LinearRegressionTuningPipeline, RandomForestPipeline}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val rawFilePath: ScallopOption[String] = trailArg[String](required = false)
  verify()
}

object FlightDelayApp {

  private val DEFAULT_FILE_PATH: String = "raw/tuning.csv"

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val filePath = conf.rawFilePath.getOrElse(DEFAULT_FILE_PATH)

    val rawDf = CSVReader.read(filePath, hasHeader = true)

    // TODO: would be super cool that this class returns the best params
    val lrTuningPipeline = new LinearRegressionTuningPipeline(rawDf)
    lrTuningPipeline.run()

    val lrPipeline = new LinearRegressionPipeline(rawDf)
    lrPipeline.run()

  }

}