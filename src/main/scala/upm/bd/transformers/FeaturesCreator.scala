package upm.bd.transformers

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * This transformer creates a new column called 'features' when the
  * [[upm.bd.transformers.FeaturesCreator#transform transform]] method is called.
  *
  * @param featuresColNames An array with the names of the columns that are going to
  *                         be aggregated under the 'features' column.
  */
class FeaturesCreator(val featuresColNames: Array[String]) extends VectorAssembler {

  override def transform(dataset: Dataset[_]): DataFrame = {
    this.setInputCols(featuresColNames)
    this.setOutputCol("features")
    super.transform(dataset)
  }

}

