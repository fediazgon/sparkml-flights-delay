package upm.bd.pipelines

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Dataset
import upm.bd.utils.MyLogger

class LinearRegressionPipeline(data: Dataset[_])
  extends PipelineWithPreprocessing(data) {

  override def executePipeline(data: Dataset[_]): Unit = {

    val Array(training, test) = data.randomSplit(Array(0.7, 0.3))

    import PipelineWithPreprocessing.{LABEL_COL, PREDICTION_COL, METRIC_NAME}

    val lr = new LinearRegression()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMaxIter(10)
      .setRegParam(0.1)
      .setElasticNetParam(0.5)

    MyLogger.info("Training...")
    val model = lr.fit(training)

    MyLogger.info("Testing...")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(LABEL_COL)
      .setPredictionCol(PREDICTION_COL)
      .setMetricName(METRIC_NAME)

    // Select example rows to display.
    MyLogger.info("Predictions:")
    predictions.select(LABEL_COL, PREDICTION_COL, "features").show(5)

    val metric = evaluator.evaluate(predictions)
    MyLogger.info(s"Value of $METRIC_NAME of testing subset: $metric")

  }

}
