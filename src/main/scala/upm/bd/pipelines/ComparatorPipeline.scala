package upm.bd.pipelines

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Dataset
import upm.bd.utils.MyLogger

class ComparatorPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  override def executePipeline(data: Dataset[_]): Unit = {

    val Array(training, inTheLocker) = data.randomSplit(Array(0.7, 0.3))

    import PipelineWithPreprocessing.{LABEL_COL, METRIC_NAME, PREDICTION_COL}

    val lr = new LinearRegression()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.2)

    val rf = new RandomForestRegressor()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMaxDepth(10)
      .setNumTrees(34)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMetricName(METRIC_NAME)

    val emptyParamGrid = new ParamGridBuilder().build()

    MyLogger.info("Training linear regression")
    val cvLrModel =
      getModelFromCrossValidation(
        new CrossValidator()
          .setEstimator(lr)
          .setEvaluator(evaluator)
          .setEstimatorParamMaps(emptyParamGrid)
          .setNumFolds(10),
        training)

    MyLogger.info("Training random forest")
    val cvRfModel =
      getModelFromCrossValidation(
        new CrossValidator()
          .setEstimator(rf)
          .setEvaluator(evaluator)
          .setEstimatorParamMaps(emptyParamGrid)
          .setNumFolds(10),
        training)

    val bestLrModel = cvLrModel.bestModel
    val bestRfModel = cvRfModel.bestModel

    val avgLrAccuracy = cvLrModel.avgMetrics.min
    val avgRfAccuracy = cvRfModel.avgMetrics.min

    MyLogger.info(s"Linear Regression accuracy = $avgLrAccuracy")
    MyLogger.info(s"Random Forest accuracy = $avgRfAccuracy")

    val winner = if (avgLrAccuracy < avgRfAccuracy) bestLrModel else bestRfModel
    MyLogger.info(s"Winner model:\n" +
      s"${winner.parent.extractParamMap()}")

    MyLogger.info("Evaluating winner model")
    val predictions = winner.transform(inTheLocker)

    // Select example rows to display.
    MyLogger.info("Predictions:")
    predictions.select(LABEL_COL, PREDICTION_COL, "features").show(5)

    val metric = evaluator.evaluate(predictions)
    MyLogger.info(s"Value of $METRIC_NAME of testing subset: $metric")

  }

}
