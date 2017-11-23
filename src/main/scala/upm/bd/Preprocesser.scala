package upm.bd

import org.apache.spark.sql.DataFrame

class Preprocesser {
  def preprocess(filePath : String) : DataFrame =
  {
    SparkSessionWrapper.spark.read.csv(filePath)
  }

}
