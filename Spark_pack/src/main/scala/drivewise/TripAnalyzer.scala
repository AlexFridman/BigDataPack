package drivewise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.json4s.JsonDSL._
import org.json4s.jackson.Json._
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.elasticsearch.spark.sql._

import org.elasticsearch.spark._

object TripAnalyzer {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local", "Trip Analyzer", new SparkConf().setAppName("TripAnalyzer"))
    val sqlContext = new SQLContext(sparkContext)
    // sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

    val data = sqlContext.read.json("/opt/Iphone.json")

    data.registerTempTable("IphoneData")
    val infoRow = sqlContext.sql("select payload.tripID, dataId,timeSubmitted,payload.tripstarttime from IphoneData")
    //  dataIdRow.rdd.saveAsTextFile("/opt/we")
    val altitude = sqlContext.sql("select payload.blackboxdetails.gpslocations.ALT from IphoneData ")

    //  output.rdd.saveAsTextFile("/opt/we") //saves data into textfile in wrapped array format  since sql query returns that

    // Converting row or wrapped array to scala list
    val l = altitude.toDF().rdd.map { case Row(x: Seq[Double]) => (x.toList) } //x.min, x.foreach { println }
    val altitudeList = l.flatMap { l => l }
    val minimum = altitudeList.min()
    val maximum = altitudeList.max()

    val tripID = infoRow.collect()(0).getString(0) //getting data as String or scala type from sql Row
    val dataId = infoRow.collect()(0).getString(1)
    val timeSubmitted = infoRow.collect()(0).getString(2)
    val tripstarttime = infoRow.collect()(0).getString(3)

    val altitudeInfo = ("Altitude" -> ("dataId" -> dataId) ~ ("timeSubmitted" -> timeSubmitted.toString()) ~ ("tripstarttime" -> tripstarttime.toString()) ~ ("tripID" -> tripID)
      ~ ("Minimum_Alt" -> minimum) ~ ("Maximum_Alt" -> maximum))

    // sparkContext.makeRDD(Seq(altitudeInfo)).saveToEs("trips/docs")  //writing data to elasticsearch 

    val jsonResult = Json(DefaultFormats).write(altitudeInfo)
    sparkContext.parallelize(List(jsonResult)).saveAsTextFile("/opt/test")

    //    sparkContext.parallelize(List(json)).saveAsTextFile("/opt/test") //converts list into rdd  and saves as text file

    // output.toJSON.saveAsTextFile("/opt/test1") //sql row into json conversion

    //  l.map { x => x }.saveAsTextFile("/opt/test1") //saves the whole list

    //    val min = l.map { x => x.min } //calculates minimum value
    //    val max = l.map { x => x.max } //calculates minimum value

    //  l.map { x => println("Minimum ALT = " + x.min + "  Maximum ALT =" + x.max + "  Average ALT =" + x.sum / x.size) }.collect()

    //val df = sqlContext.load("org.apache.spark.sql.json", Map("path" -> "/opt/IPhone6s.json"))
  }
  case class MinMax(min: Double, max: Double)
}