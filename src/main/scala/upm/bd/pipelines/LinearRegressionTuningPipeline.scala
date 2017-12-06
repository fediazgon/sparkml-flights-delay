package upm.bd.pipelines

import org.apache.spark.ml.Model
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.Dataset
import upm.bd.utils.MyLogger
import PipelineWithPreprocessing.{LABEL_COL, METRIC_NAME, PREDICTION_COL}
import org.apache.spark.ml.param.ParamMap

class LinearRegressionTuningPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  override def executePipeline(data: Dataset[_]): Unit = {

    val Array(training, test) = data.randomSplit(Array(0.7, 0.3))

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

    val crossValidator = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    // val bestModel = getBestModelFromTrainValidation(trainValidationSplit, data)
    val bestModel = getBestModelFromCrossValidation(crossValidator, data)

    MyLogger.info("Best model: \n" +
      s"${bestModel.parent.extractParamMap()}")
    MyLogger.info("Testing against best model...")

    val predictions = bestModel.transform(test)

    // Select example rows to display.
    MyLogger.info("Predictions:")
    predictions.select(LABEL_COL, PREDICTION_COL, "features").show(5)

  }

  private def getBestModelFromTrainValidation(trainValidationSplit: TrainValidationSplit,
                                              trainingData: Dataset[_]): Model[_] = {
    MyLogger.info("Training...")
    val model = trainValidationSplit.fit(trainingData)

    val trainedModelParams = model.getEstimatorParamMaps
    MyLogger.info(s"${trainedModelParams.length} models were trained. " +
      s"Showing $METRIC_NAME value for each one:")

    printModelsWithMetrics(trainedModelParams, model.validationMetrics)

    model.bestModel
  }

  private def getBestModelFromCrossValidation(crossValidator: CrossValidator,
                                              trainingData: Dataset[_]): Model[_] = {
    MyLogger.info("Training...")
    val model = crossValidator.fit(trainingData)

    val trainedModelParams = model.getEstimatorParamMaps
    MyLogger.info(s"${trainedModelParams.length} models were trained. " +
      s"Showing avg $METRIC_NAME value for each one:")

    printModelsWithMetrics(trainedModelParams, model.avgMetrics)

    model.bestModel

  }

  private def printModelsWithMetrics(trainedModelParams: Array[ParamMap],
                                     metricValues: Array[Double]): Unit = {
    trainedModelParams.zip(metricValues).zipWithIndex.foreach {
      case ((params, metric), index) =>
        MyLogger.info(s"Model ${index + 1}:\n" +
          s"$params -> value = $metric")
    }
  }

}
