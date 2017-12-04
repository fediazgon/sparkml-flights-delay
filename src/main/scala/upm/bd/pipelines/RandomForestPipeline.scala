package upm.bd.pipelines

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, Row}
import upm.bd.transformers.{FeaturesCreator, Indexer, Preprocesser}
import org.apache.spark.sql.functions._
import upm.bd.utils.MyLogger


class RandomForestPipeline(val data: Dataset[_]) {

  val INDEX_COL_NAMES = Array("UniqueCarrier", "Origin", "Dest", "Route")


  val FEATURES_COL_NAMES = Array(
    "Month",
    "DayofMonth",
//    "DayOfWeek",
    "Distance",
    "TaxiOut",
    "UniqueCarrierIndex",
//    "OriginIndex",
//    "DestIndex",
    "WeekEnd",
    "CRSDepTimeMin",
    "DepTimeMin",
    "CRSElapsedTime"
  )

  def run() : Unit =
  {
    import upm.bd.utils.SparkSessionWrapper.spark.implicits._

    val preprocesser = new Preprocesser(verbose = false)
    val preprocessedDf = preprocesser.transform(data)

    val indexed = new Indexer(INDEX_COL_NAMES).transform(preprocessedDf)

    val featuresCreator = new FeaturesCreator(FEATURES_COL_NAMES)
    val dfFeatures = featuresCreator.transform(indexed)

    val indexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)
      .fit(dfFeatures)

    val finalData = indexerModel.transform(dfFeatures)//.select($"indexed",$"ArrDelay")
    //val finalData = dfFeatures
    MyLogger.info("Final Data")
    finalData.show(10)

    val Array(training, test) = finalData.randomSplit(Array(0.7, 0.3))

    val model = new RandomForestRegressor()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("indexed")
//      .setMaxBins(1000)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val paramGrid = new ParamGridBuilder()
      .build()

    val cv = new CrossValidator()
      .setEstimator(model)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val cvModel = cv.fit(training)
    println(cvModel.avgMetrics.foreach(println))

    cvModel.transform(test).select($"ArrDelay",$"prediction").show(100)
    //    .select("id", "text", "probability", "prediction")
    //    .collect()
    //    .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
    //      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    //    }
  }
}
