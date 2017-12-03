package upm.bd

import org.apache.spark.sql.Dataset
import upm.bd.utils.{MyLogger, SparkSessionWrapper}

object CSVReader {

  def read(filePath: String, hasHeader: Boolean, sampleRate: Double = 1): Dataset[_] = {

    MyLogger.info(s"Reading file $filePath")
    var df =
      SparkSessionWrapper.spark
        .read
        .option("header", value = hasHeader)
        .csv(filePath)

    if (sampleRate != 1) {
      df = df.sample(true, sampleRate)
    }

    df
  }

}
