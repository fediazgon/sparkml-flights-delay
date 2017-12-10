package upm.bd.pipelines

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.Dataset
import upm.bd.utils.MyLogger
import org.apache.spark.sql.functions._

class LogisticRegressionPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  val WEIGHT_COL = "OverDelayWeightCol"
  val LABEL_COL = "OverDelay"
  val RAW_PREDICTION_COL = "rawPrediction"
  val METRIC_NAME = "areaUnderROC"

  override def executePipeline(data: Dataset[_]): Unit = {

    MyLogger.info("Balancing dataset because lots of negative examples (i.e., no over delay)")
    val balancedDataset = balanceDataset(data)
    balancedDataset.show(10)

    val Array(training, inTheLocker) = balancedDataset.randomSplit(Array(0.7, 0.3))

    val lgr = new LogisticRegression()
      .setWeightCol(WEIGHT_COL)
      .setLabelCol(LABEL_COL)
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.2)

    MyLogger.info("Training...")
    val model = lgr.fit(training)

    MyLogger.info("Testing...")
    val predictions = model.transform(inTheLocker)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(LABEL_COL)
      .setRawPredictionCol(RAW_PREDICTION_COL)
      .setMetricName(METRIC_NAME)

    // Select example rows to display.
    MyLogger.info("Predictions:")
    predictions.select("ArrDelay", WEIGHT_COL, LABEL_COL, RAW_PREDICTION_COL,
      "prediction", "probability", "features").show(15)

    val metric = evaluator.evaluate(predictions)
    MyLogger.info(s"Value of $METRIC_NAME of testing subset: $metric")

  }

  def balanceDataset(dataset: Dataset[_]): Dataset[_] = {

    // We want to under sample the negatives
    // Re-balancing (weighting) of records to be used in the logistic loss objective function
    val numPositives = dataset.filter(dataset(LABEL_COL) === 1).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numPositives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 0.0) {
        1 * balancingRatio
      }
      else {
        1 * (1.0 - balancingRatio)
      }
    }

    val weightedDataset = dataset.withColumn(WEIGHT_COL, calculateWeights(dataset(LABEL_COL)))
    weightedDataset
  }

}
