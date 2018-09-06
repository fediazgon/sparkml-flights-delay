package fdiazgon.pipelines

import fdiazgon.pipelines.PipelineWithPreprocessing.{LABEL_COL, METRIC_NAME, PREDICTION_COL}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Dataset

class LinearRegressionTuningPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  override def executePipeline(data: Dataset[_]): Unit = {

    val lr = new LinearRegression()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMaxIter(10)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMetricName(METRIC_NAME)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.3, 0.1, 0.01))
      .addGrid(lr.elasticNetParam, Array(0.2, 0.5, 0.8))
      .build()

    //    val model =
    //      getModelFromTrainValidation(
    //        new TrainValidationSplit()
    //          .setEstimator(lr)
    //          .setEvaluator(evaluator)
    //          .setEstimatorParamMaps(paramGrid)
    //          .setTrainRatio(0.8),
    //        data)

    val model =
      getModelFromCrossValidation(
        new CrossValidator()
          .setEstimator(lr)
          .setEvaluator(evaluator)
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(10),
        data)

    val bestModel = model.bestModel
    logger.info("Best model: \n" +
      s"${bestModel.parent.extractParamMap()} -> value = ${model.avgMetrics.min}")

  }

}
