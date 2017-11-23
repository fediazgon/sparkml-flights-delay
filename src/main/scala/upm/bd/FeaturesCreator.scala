package upm.bd

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StringType}
import upm.bd.{DataFrameUtils => dutils}

class FeaturesCreator extends VectorAssembler {

  // Maybe this could be parametrized
  private val featuresCols = Array("Year", "Month", "DayOfWeek")
  private val targetCol = "features"

  override def transform(dataset: Dataset[_]): DataFrame = {
    val casted = dutils.castAllTypedColumnsTo(dataset.toDF(), StringType, IntegerType)
    this.setInputCols(featuresCols)
    this.setOutputCol(targetCol)
    super.transform(casted)
  }


}

