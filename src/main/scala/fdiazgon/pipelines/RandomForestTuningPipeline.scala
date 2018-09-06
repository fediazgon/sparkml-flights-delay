package fdiazgon.pipelines

import fdiazgon.pipelines.PipelineWithPreprocessing.{LABEL_COL, METRIC_NAME, PREDICTION_COL}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Dataset

class RandomForestTuningPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  override def executePipeline(data: Dataset[_]): Unit = {

    val rf = new RandomForestRegressor()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMetricName(METRIC_NAME)

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(5, 8, 10))
      .addGrid(rf.numTrees, Array(24, 34))
      .build()

    //    val model =
    //      getModelFromTrainValidation(
    //        new TrainValidationSplit()
    //          .setEstimator(rf)
    //          .setEvaluator(evaluator)
    //          .setEstimatorParamMaps(paramGrid)
    //          .setTrainRatio(0.8),
    //        data)

    val model =
      getModelFromCrossValidation(
        new CrossValidator()
          .setEstimator(rf)
          .setEvaluator(evaluator)
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(10),
        data)

    val bestModel = model.bestModel
    logger.info("Best model: \n" +
      s"${bestModel.parent.extractParamMap()} -> value = ${model.avgMetrics.min}")

  }

}
