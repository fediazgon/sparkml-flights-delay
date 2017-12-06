package upm.bd.pipelines

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.Dataset
import upm.bd.pipelines.PipelineWithPreprocessing.{LABEL_COL, METRIC_NAME, PREDICTION_COL}
import upm.bd.utils.MyLogger

class RandomForestTuningPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

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
    MyLogger.info("Best model: \n" +
      s"${bestModel.parent.extractParamMap()} -> value = ${model.avgMetrics.min}")

  }

}
