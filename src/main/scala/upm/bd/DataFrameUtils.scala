package upm.bd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

object DataFrameUtils {

  def castAllTypedColumnsTo(df: DataFrame, sourceType: DataType,
                            targetType: DataType): DataFrame = {
    df.schema.filter(_.dataType == sourceType).foldLeft(df)(
      (acc, col) => acc.withColumn(col.name, df(col.name).cast(targetType))
    )
  }

}
