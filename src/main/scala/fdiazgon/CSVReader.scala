package fdiazgon

import fdiazgon.utils.SparkSessionWrapper
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Dataset

object CSVReader {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  def read(filePath: String, hasHeader: Boolean, sampleRate: Double = 1): Dataset[_] = {

    logger.info(s"Reading file $filePath")
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
