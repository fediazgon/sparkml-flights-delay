package fdiazgon

import fdiazgon.pipelines.{ComparatorPipeline, LinearRegressionPipeline, LinearRegressionTuningPipeline, RandomForestTuningPipeline}
import org.rogach.scallop.{ScallopOption, _}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val rawFilePath: ScallopOption[String] = trailArg[String](required = false)
  val tuning: ScallopOption[Boolean] = opt[Boolean]()
  val compare: ScallopOption[Boolean] = opt[Boolean]()
  val explore: ScallopOption[Boolean] = opt[Boolean]()
  verify()
}

object FlightsDelayApp {

  private val TUNING_FILE_PATH: String = "raw/tuning.csv"

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val filePath = conf.rawFilePath.getOrElse(TUNING_FILE_PATH)
    val shouldExplore = conf.explore.supplied
    val shouldTune = conf.tuning.supplied
    val shouldCompare = conf.compare.supplied

    val rawDf = CSVReader.read(filePath, hasHeader = true)

    if (shouldExplore) new Explorer().explore(rawDf)

    if (shouldTune) {
      val tuneDf =
        if (filePath != TUNING_FILE_PATH)
          CSVReader.read(TUNING_FILE_PATH, hasHeader = true)
        else
          rawDf
      new LinearRegressionTuningPipeline(tuneDf).run()
      new RandomForestTuningPipeline(tuneDf).run()
    }

    if (shouldCompare) new ComparatorPipeline(rawDf).run()

    new LinearRegressionPipeline(rawDf).run()

  }
}