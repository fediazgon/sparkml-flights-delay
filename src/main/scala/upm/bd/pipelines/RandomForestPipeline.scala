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
//    "Month",
//    "DayofMonth",
//    "DayOfWeek",
//    "WeekEnd",
    "Distance",
    "TaxiOut",
//    "UniqueCarrierIndex",
//    "OriginIndex",
//    "DestIndex",
//    "CRSDepTimeMin",
    "DepDelay",
    "DepTimeMin"
//    "CRSElapsedTime"
  )

  def run() : Unit =
  {
    import upm.bd.utils.SparkSessionWrapper.spark.implicits._

    val preprocesser = new Preprocesser(verbose = false)
    val preprocessedDf = preprocesser.preprocess(data)

    val indexedDf = new Indexer(INDEX_COL_NAMES).indexColumns(preprocessedDf)

    val featuresCreator = new FeaturesCreator(FEATURES_COL_NAMES)
    val featuresDf = featuresCreator.transform(indexedDf)

//    val indexerModel = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexed")
//      .setMaxCategories(10)
//      .fit(featuresDf)

//    val finalData = indexerModel.transform(featuresDf)//.select($"indexed",$"ArrDelay")
    var finalData = featuresDf
    MyLogger.info("Final Data")
    finalData.show(100,truncate = false)

    finalData = finalData.select($"features",$"ArrDelay")


    val Array(training, test) = finalData.randomSplit(Array(0.7, 0.3))

    val model = new RandomForestRegressor()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("features")
//      .setMaxBins(1000)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val paramGrid = new ParamGridBuilder()
      .build()

    val cv = new CrossValidator()
      .setEstimator(model)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    MyLogger.printHeader("TRAINING")
    val cvModel = cv.fit(training)
    println(cvModel.avgMetrics.foreach(println))

    MyLogger.printHeader("TESTING")
    cvModel.transform(test)
      .select($"ArrDelay",$"prediction", (abs($"ArrDelay" - $"prediction")).as("residual")).select(mean($"residual").as("MAE"), stddev($"residual")).show()
    //    .select("id", "text", "probability", "prediction")1
    //    .collect()
    //    .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
    //      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    //    }
  }
}
