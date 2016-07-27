package drivewise
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.JsonMethods.render
import org.json4s.JsonDSL.WithDouble._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._

object Mapping {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ElatisSpark")
    val sparkContext = new SparkContext("local", "Spark-Elastic", sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val data = sqlContext.read.json("/opt/color.json")

    data.saveToEs("data/docs")

    //    val numbers = Map("one" -> 1, "two" -> 2)
    //    val trips = Map("Chicago" -> "June", "New york" -> "September")
    //
    //    sparkContext.makeRDD(Seq(numbers, trips)).saveToEs("spark/docs")

  }
}