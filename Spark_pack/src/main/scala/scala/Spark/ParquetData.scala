package scala.Spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark._
import org.apache.spark.sql.SQLContext
/**
 * This reads data on json format with parquet format and stores in a file
 * now the data on output file will be readable format
 */
object ParquetData {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local", "json parquet", new SparkConf().setAppName("Json to Parquet"))

    val sqlContext = new SQLContext(sparkContext)
    val parq = sqlContext.read.parquet("hdfs://localhost:9000//user/dpanw/kafka/file2")
    val parq1 = sqlContext.read.parquet("hdfs://localhost:9000//user/dpanw/kafka/file3")

    parq.registerTempTable("twitter")
    parq1.registerTempTable("twitter1")

    val result = sqlContext.sql("SELECT * from twitter")
    val result1 = sqlContext.sql("SELECT * from twitter1")

    result.rdd.map { x => x }.saveAsTextFile("/opt/data1")
    result1.rdd.map { x => x }.saveAsTextFile("/opt/data2")

    // result1.show()
  }
}
