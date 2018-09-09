package fdiazgon

import fdiazgon.pipelines.{LinearRegressionTuningPipeline, RandomForestTuningPipeline}
import fdiazgon.utils.{Constants, DataPreparation, LoggingUtils, SparkSessionWrapper}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.immutable.HashMap

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val file: ScallopOption[String] = trailArg[String](required = true)
  val explore: ScallopOption[Boolean] = opt[Boolean]()
  verify()
}

object FlightsDelayApp extends SparkSessionWrapper {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val conf = new Conf(args)
    val filePath = conf.file()
    val shouldExplore = conf.explore.supplied

    logger.info(s"Reading file $filePath")
    val data = spark.read.format("csv").option("header", value = true).csv(filePath)
      .withColumn("Year", $"Year".cast("int"))
      .withColumn("Month", $"Month".cast("int"))
      .withColumn("DayofMonth", $"DayofMonth".cast("int"))
      .withColumn("DayOfWeek", $"DayOfWeek".cast("int"))
      .withColumn("CRSElapsedTime", $"CRSElapsedTime".cast("int"))
      .withColumn("DepDelay", $"DepDelay".cast("int"))
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("TaxiOut", $"TaxiOut".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int"))
      .withColumn("Diverted", $"Diverted".cast("int"))
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))

    if (shouldExplore) DataPreparation.explore(data)

    val Array(training, inTheLocker) = DataPreparation.preprocess(data).randomSplit(Array(0.7, 0.3))

    val (bestLrModel, lrEval) = new LinearRegressionTuningPipeline().fit(training)
    val (bestRfModel, rfEval) = new RandomForestTuningPipeline().fit(training)

    val models: HashMap[PipelineModel, Double] = HashMap(
      (bestLrModel, lrEval),
      (bestRfModel, rfEval)
    )

    LoggingUtils.printHeader("Comparison results")
    logger.info(s"Best Linear Regression ${Constants.METRIC_NAME} value = $lrEval")
    logger.info(s"Best Random Forest ${Constants.METRIC_NAME} value = $rfEval")

    logger.info("Evaluating winner model on test set")
    val bestModel: PipelineModel = models.minBy(_._2)._1
    val predictions = bestModel.transform(inTheLocker)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(Constants.LABEL_COL)
      .setPredictionCol(Constants.PREDICTION_COL)
      .setMetricName(Constants.METRIC_NAME)

    val testEval = evaluator.evaluate(predictions)
    logger.info(s"Value of ${Constants.METRIC_NAME} on testing subset: $testEval")
  }

}