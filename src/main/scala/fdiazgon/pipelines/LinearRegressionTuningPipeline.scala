package fdiazgon.pipelines

import fdiazgon.utils.Constants
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder

class LinearRegressionTuningPipeline extends CVTuningPipeline {

  override def getEstimatorAndParams: (PipelineStage, Array[ParamMap]) = {

    val lr = new LinearRegression()
      .setLabelCol(Constants.LABEL_COL)
      .setPredictionCol(Constants.PREDICTION_COL)
      .setMaxIter(10)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.3, 0.1, 0.01))
      .addGrid(lr.elasticNetParam, Array(0.2, 0.5, 0.8))
      .build()

    (lr, paramGrid)
  }

}
