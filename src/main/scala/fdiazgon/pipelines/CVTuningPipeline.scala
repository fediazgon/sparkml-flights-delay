package fdiazgon.pipelines

import fdiazgon.utils.{Constants, LoggingUtils}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.Dataset

/**
  * <p>
  * Base pipeline that preprocess the data indexing string columns and grouping columns that are going to be used as
  * features. The specific columns that are processed can be found in the companion object `CVTuningPipeline`.
  * </p>
  *
  * <p>
  * Any class extending this one must implement the method `getEstimatorAndParams` to return the estimator to be used
  * and the grid of hyperparameters to tune.
  * </p>
  *
  */
abstract class CVTuningPipeline {

  private val logger: Logger = LogManager.getLogger("mylogger")

  /**
    * Preprocess the data and executes the pipeline.
    *
    * @param dataset Fits a model to the input data.
    * @return the best model along with the value achieved in the evaluation metric.
    */
  def fit(dataset: Dataset[_]): (PipelineModel, Double) = {

    LoggingUtils.printHeader(s"Running ${this.getClass.getSimpleName}")
    logger.info(s"Features are: ${Constants.FEATURES_INPUT_COLS.mkString(", ")}")

    var stages = Array[PipelineStage]()

    Constants.INDEX_INPUT_COLS.foreach { inputCol =>
      stages :+= new StringIndexer()
        .setInputCol(inputCol)
        .setOutputCol(inputCol + Constants.INDEXED_COL_SUFFIX)
    }

    stages :+= new VectorAssembler()
      .setInputCols(Constants.FEATURES_INPUT_COLS)
      .setOutputCol(Constants.FEATURES_OUTPUT_COL)

    val (estimator, paramGrid) = getEstimatorAndParams
    stages :+= estimator

    val mainPipeline = new Pipeline().setStages(stages)

    val cv = new CrossValidator()
      .setEstimator(mainPipeline)
      .setEvaluator(new RegressionEvaluator()
        .setLabelCol(Constants.LABEL_COL)
        .setPredictionCol(Constants.PREDICTION_COL)
        .setMetricName(Constants.METRIC_NAME))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val cvModel = cv.fit(dataset)
    printModelsWithMetrics(cvModel)

    (cvModel.bestModel.asInstanceOf[PipelineModel], cvModel.avgMetrics.min)
  }

  private def printModelsWithMetrics(cvModel: CrossValidatorModel): Unit = {
    val avgMetrics = cvModel.avgMetrics
    logger.info(s"A total of ${avgMetrics.length} models were trained. Showing results for each model")
    cvModel.getEstimatorParamMaps.zip(avgMetrics).zipWithIndex.foreach {
      case ((params, metric), index) =>
        logger.info(s"Model ${index + 1}:\n$params -> ${Constants.METRIC_NAME} value = $metric")
    }
  }

  protected def getEstimatorAndParams: (PipelineStage, Array[ParamMap])

}

