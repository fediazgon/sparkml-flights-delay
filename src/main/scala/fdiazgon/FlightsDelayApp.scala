package fdiazgon

import fdiazgon.pipelines.{ComparatorPipeline, LinearRegressionPipeline, LinearRegressionTuningPipeline, RandomForestTuningPipeline}
import org.rogach.scallop.{ScallopOption, _}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val file: ScallopOption[String] = trailArg[String](required = true)
  val tuning: ScallopOption[Boolean] = opt[Boolean]()
  val compare: ScallopOption[Boolean] = opt[Boolean]()
  val explore: ScallopOption[Boolean] = opt[Boolean]()
  val logistic: ScallopOption[Boolean] = opt[Boolean]()
  verify()
}

object FlightsDelayApp {

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val filePath = conf.file()
    val shouldExplore = conf.explore.supplied
    val shouldTune = conf.tuning.supplied
    val shouldCompare = conf.compare.supplied

    lazy val rawDf = CSVReader.read(filePath, hasHeader = true)

    if (shouldExplore) new Explorer().explore(rawDf)

    if (shouldTune) {
      new LinearRegressionTuningPipeline(rawDf).run()
      new RandomForestTuningPipeline(rawDf).run()
    }

    if (shouldCompare) new ComparatorPipeline(rawDf).run()

    new LinearRegressionPipeline(rawDf).run()

  }

}