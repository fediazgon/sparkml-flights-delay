package fdiazgon

import fdiazgon.utils.SparkSessionWrapper
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Dataset

object CSVReader extends SparkSessionWrapper {

  private[this] val logger: Logger = LogManager.getLogger("mylogger")

  def read(filePath: String, header: Boolean): Dataset[_] = {
    logger.info(s"Reading file $filePath")
    spark
      .read
      .option("header", value = header)
      .csv(filePath)
  }

}
