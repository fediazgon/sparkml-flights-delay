package fdiazgon.pipelines

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.Dataset


class RandomForestPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  override def executePipeline(data: Dataset[_]): Unit = {

    val Array(training, inTheLocker) = data.randomSplit(Array(0.7, 0.3))

    import PipelineWithPreprocessing.{LABEL_COL, METRIC_NAME, PREDICTION_COL}

    val rf = new RandomForestRegressor()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMaxDepth(10)
      .setNumTrees(34)

    logger.info("Training...")
    val model = rf.fit(training)

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
