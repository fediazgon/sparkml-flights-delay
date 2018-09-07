package fdiazgon

import fdiazgon.pipelines._
import org.rogach.scallop.{ScallopConf, ScallopOption}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val file: ScallopOption[String] = trailArg[String](required = true)
  val explore: ScallopOption[Boolean] = opt[Boolean]()
  val tune: ScallopOption[Boolean] = opt[Boolean]()
  val compare: ScallopOption[Boolean] = opt[Boolean]()
  verify()
}

object FlightsDelayApp {

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val filePath = conf.file()
    val shouldExplore = conf.explore.supplied
    val shouldTune = conf.tune.supplied
    val shouldCompare = conf.compare.supplied

    val rawDf = CSVReader.read(filePath, header = true)

    if (shouldExplore) new Explorer().explore(rawDf)

    if (shouldTune) {
      new LinearRegressionTuningPipeline(rawDf).run()
      new RandomForestTuningPipeline(rawDf).run()
    }

    if (shouldCompare) new ComparatorPipeline(rawDf).run()

    new LinearRegressionPipeline(rawDf).run()

  }

}