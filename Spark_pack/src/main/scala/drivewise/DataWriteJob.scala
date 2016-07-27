package drivewise

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/*
 *input data as json file and writes to hdfs on parquet format, step ahead it uses snappy compression
 * Command:
 *  spark-submit --class drivewise.DataWriteJob --master local /Users/dpanw/Documents/workspace/Spark/target/Spark-0.0.1-SNAPSHOT.jar
 */

object DataWriteJob {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WriteData")
    val sparkContext = new SparkContext("local", "WriteData", sparkConf)

    val sqlContext = new SQLContext(sparkContext)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

 //  val inputData = sqlContext.read.json("/opt/Iphone.json") //SOURCE
 //   inputData.write.parquet("hdfs://localhost:9000/user/dpanw/result") //DESTINATION

      val output = sqlContext.read.parquet("/opt/KafkaOutput/data1")
        output.write.json("/opt/test")
  }
}
