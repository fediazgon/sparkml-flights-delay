package upm.bd.pipelines

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.Dataset
import upm.bd.utils.MyLogger

class LinearRegressionTuningPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  override def executePipeline(data: Dataset[_]): Unit = {

    val Array(training, test) = data.randomSplit(Array(0.7, 0.3))

    import PipelineWithPreprocessing.{LABEL_COL, PREDICTION_COL, METRIC_NAME}

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

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    MyLogger.info("Training...")
    val model = trainValidationSplit.fit(training)

    val trainedModelParams = model.getEstimatorParamMaps
    MyLogger.info(s"${trainedModelParams.length} models were trained. " +
      s"Showing $METRIC_NAME value for each one:")

    trainedModelParams.zip(model.validationMetrics).zipWithIndex.foreach {
      case ((params, metric), index) =>
        MyLogger.info(s"Model ${index + 1}:\n" +
          s"$params -> value = $metric")
    }

    MyLogger.info("Best model: \n" +
      s"${model.bestModel.parent.extractParamMap()}")
    MyLogger.info("Testing against best model...")

    val predictions = model.transform(test)

    // Select example rows to display.
    MyLogger.info("Predictions:")
    predictions.select(LABEL_COL, PREDICTION_COL, "features").show(5)

  }

}
