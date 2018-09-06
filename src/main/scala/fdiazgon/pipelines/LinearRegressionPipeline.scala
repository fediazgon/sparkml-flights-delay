package fdiazgon.pipelines

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Dataset

class LinearRegressionPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  override def executePipeline(data: Dataset[_]): Unit = {

    val Array(training, inTheLocker) = data.randomSplit(Array(0.7, 0.3))

    import PipelineWithPreprocessing.{LABEL_COL, METRIC_NAME, PREDICTION_COL}

    val lr = new LinearRegression()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.2)

    logger.info("Training...")
    val model = lr.fit(training)

    logger.info("Testing...")
    val predictions = model.transform(inTheLocker)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMetricName(METRIC_NAME)

    // Select example rows to display.
    logger.info("Predictions:")
    predictions.select(LABEL_COL, PREDICTION_COL, "features").show(5)

    val metric = evaluator.evaluate(predictions)
    logger.info(s"Value of $METRIC_NAME of testing subset: $metric")

  }

}
