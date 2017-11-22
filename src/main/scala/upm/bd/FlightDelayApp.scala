import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//  val apples = opt[Int](required = true)
//  val bananas = opt[Int]()
  val rawFilePath = trailArg[String](required = true)
  verify()
}

object FlightDelayApp {
  def main(args : Array[String]): Unit =
  {
    println("hello World!")
    val conf = new Conf(args)
    println(s"Using file ${conf.rawFilePath.getOrElse("")}")
    
  }

}