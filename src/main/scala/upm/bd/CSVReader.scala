package upm.bd

import org.apache.spark.sql.Dataset
import upm.bd.utils.{MyLogger, SparkSessionWrapper}

object CSVReader {

  def read(filePath: String, hasHeader: Boolean): Dataset[_] = {

    MyLogger.info(s"Reading file $filePath")
    SparkSessionWrapper.spark
      .read
      .option("header", value = hasHeader)
      .csv(filePath)

  }

}
