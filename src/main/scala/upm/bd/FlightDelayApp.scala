package upm.bd

import org.apache.spark.ml.regression.LinearRegression
import org.rogach.scallop._
import upm.bd.transformers.{FeaturesCreator, Preprocesser}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val rawFilePath: ScallopOption[String] = trailArg[String](required = false)
  verify()
}

object FlightDelayApp {

  private val DEFAULT_FILE_PATH: String = "raw/2008.csv.bz2"

  def main(args: Array[String]): Unit = {

    val FEATURES_COL_NAMES = Array("Year", "Month", "DayOfWeek")
    val TARGET_COL_NAMES = "ArrDelay"

    val conf = new Conf(args)
    val rawFilePath = conf.rawFilePath.getOrElse(DEFAULT_FILE_PATH)

    val preprocesser = new Preprocesser
    val preprocessedDf = preprocesser.preprocess(rawFilePath)

    val explorer = new Explorer
    explorer.explore(preprocessedDf)

    val featuresCreator = new FeaturesCreator(FEATURES_COL_NAMES)
    val dfFeatures = featuresCreator.transform(preprocessedDf)

    val lr = new LinearRegression()
      .setFeaturesCol(FeaturesCreator.FEATURES_COL)
      .setLabelCol(TARGET_COL_NAMES)
      .setMaxIter(10)
      .setElasticNetParam(0.8)
    val lrModel = lr.fit(dfFeatures)
    print(s"Summary: ${lrModel.summary.residuals}")

  }

}