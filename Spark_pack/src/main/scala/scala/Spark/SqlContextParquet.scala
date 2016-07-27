package scala.Spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark._
import org.apache.spark.sql.SQLContext

object SqlContextParquet {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local", "json parquet", new SparkConf().setAppName("Json to Parquet"))

    val sqlContext = new SQLContext(sparkContext)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    import sqlContext.implicits._

    //    
    //  val data = sqlContext.read.json("hdfs://localhost:9000/user/dpanw/input1")
    //  data.printSchema();
    //  data.write.parquet("hdfs://localhost:9000/user/dpanw/output/parqresult")

    val parq = sqlContext.read.parquet("hdfs://localhost:9000//user/dpanw/output/parqresult")
    parq.registerTempTable("drivedata")
    val result = sqlContext.sql("SELECT * from drivedata")
    result.show()

    //   parq.write.json("hdfs://localhost:9000/user/dpanw/output/json")

    /*
    * to define the schema of userdefined object
    * 
   case class Person(name:String,age:String,id:String)
   case class Test(test:Person,age:String,id:String)
   val df= sqlContext.read.json("/opt/color.json").as[Test]
   sample data:   {"id" : "1201", "test" : {"id" : "1201", "name" : "satish", "age" : "25"}, "age" : "25"}
   */
  }
}