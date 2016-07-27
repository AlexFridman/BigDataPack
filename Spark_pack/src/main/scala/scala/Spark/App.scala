package scala.Spark

import org.json4s.JsonDSL.WithDouble.int2jvalue
import org.json4s.JsonDSL.WithDouble.pair2Assoc
import org.json4s.JsonDSL.WithDouble.pair2jvalue
import org.json4s.jackson.JsonMethods.compact
import org.json4s.jackson.JsonMethods.render
/**
 * @author ${user.name}
 */
object App {
  def main(args: Array[String]) {
    val max = 35
    val min = 9

    val json = ("minmax" -> ("minimum" -> min) ~ ("maximum" -> max))

    println(compact(render(json)))
  }
  
}
