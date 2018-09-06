import fdiazgon.transformers.FeaturesCreator
import org.apache.spark.sql.DataFrame
import org.scalatest._

class FeaturesCreatorSpec extends FlatSpec with Matchers {

  import fdiazgon.utils.SparkSessionWrapper.spark.implicits._

  val featuresColNames = Array("Year", "Month", "DayOfWeek")
  val otherColName = "Fullname"

  val df: DataFrame = Seq(
    (1993, 2, 2, "Fernando Díaz"),
    (1997, 3, 14, "Giorgio Ruffa"))
    .toDF(featuresColNames :+ otherColName: _*)

  "A FeatureCreator" must "add a 'features' column" in {
    val featuresCreator = new FeaturesCreator(featuresColNames)
    val dfOut = featuresCreator.transform(df)
    dfOut.columns should contain("features")
  }

}
