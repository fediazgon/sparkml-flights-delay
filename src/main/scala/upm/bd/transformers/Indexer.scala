package upm.bd.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class Indexer(val colNamesToIndex: Array[String]) {

  def indexColumns(dataset: Dataset[_]): DataFrame = {
    var transformed = dataset
    for (column: String <- colNamesToIndex) {
      val indexer = new StringIndexer()
        .setInputCol(column)
        .setOutputCol(column + "Index")
        .fit(transformed)
      transformed = indexer.transform(transformed)
    }
    transformed.toDF()
  }

}
