package fdiazgon.pipelines

import fdiazgon.utils.Constants
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.ParamGridBuilder

class RandomForestTuningPipeline extends CVTuningPipeline {

  override def getEstimatorAndParams: (PipelineStage, Array[ParamMap]) = {

    val rf = new RandomForestRegressor()
      .setLabelCol(Constants.LABEL_COL)
      .setPredictionCol(Constants.PREDICTION_COL)

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(5, 8, 10))
      .addGrid(rf.numTrees, Array(24, 34))
      .build()

    (rf, paramGrid)
  }

}