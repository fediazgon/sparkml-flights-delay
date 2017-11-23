import org.apache.spark.sql.DataFrame
import org.scalatest._
import upm.bd.FeaturesCreator

class FeaturesCreatorSpec extends FlatSpec with Matchers {

  import upm.bd.SparkSessionWrapper.spark.implicits._

  val featuresCols = Array("Year", "Month", "DayOfWeek")

  val df: DataFrame = Seq(
    (1993, 2, 2),
    (1997, 3, 14))
    .toDF(featuresCols: _*)

  "A FeatureCreator" must "add a 'features' column" in {
    val featuresCreator = new FeaturesCreator()
    val dfOut = featuresCreator.transform(df)

    dfOut.columns should contain("features")
  }

  "A FeatureCreator" must "not delete any column" in {
    val featuresCreator = new FeaturesCreator()
    val dfOut = featuresCreator.transform(df)

    featuresCols.foreach(
      dfOut.columns should contain(_)
    )
  }
}
