package upm.bd.pipelines

import org.apache.spark.sql.{DataFrame, Dataset}
import upm.bd.transformers.{FeaturesCreator, Indexer, Preprocesser}
import upm.bd.utils.MyLogger

/**
  * Base pipeline that preprocess the data, index some columns (strings)
  * and add a features column. The method [[upm.bd.pipelines.PipelineWithPreprocessing#executePipeline executePipeline]]
  * need to be implemented with the behavior of the pipeline.
  *
  * @param data DataFrame to apply the transformations.
  */
abstract class PipelineWithPreprocessing(val data: Dataset[_]) {

  import PipelineWithPreprocessing._

  /**
    * Creates and executes the actual pipeline.
    *
    * @param data DataFrame already preprocessed with a column called
    *             'features'. Ready to be ingested in an estimator.
    */
  def executePipeline(data: Dataset[_])

  /**
    * Preprocess the data executes the pipeline.
    */
  def run(): Unit = {
    MyLogger.printHeader(s"Running ${this.getClass.getSimpleName}")
    val preprocessedData = preprocess()
    executePipeline(preprocessedData)
    MyLogger.printHeader(s"End ${this.getClass.getSimpleName}")
  }

  private def preprocess(): Dataset[_] = {
    var df = preprocesser.preprocess(data)
    df = indexer.indexColumns(df)
    featuresCreator.transform(df)
      .cache() // TODO: to cache or not to cache
  }

}

// See https://docs.scala-lang.org/tour/singleton-objects.html
// Notes for Java programmers
object PipelineWithPreprocessing {

  private val INDEX_COL_NAMES = Array("UniqueCarrier", "Origin", "Dest", "Route")
  private val FEATURES_COL_NAMES = Array("Distance", "TaxiOut", "DepDelay", "DepTimeMin")

  val LABEL_COL = "ArrDelay"
  val PREDICTION_COL = "Predicted"
  val METRIC_NAME = "mae"

  val preprocesser = new Preprocesser()
  val indexer = new Indexer(INDEX_COL_NAMES)
  val featuresCreator = new FeaturesCreator(FEATURES_COL_NAMES)
}
