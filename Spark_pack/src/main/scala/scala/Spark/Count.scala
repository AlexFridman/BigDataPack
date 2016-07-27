

package scala.Spark
import org.apache.spark.{SparkConf, SparkContext}

object Count {
def main(args: Array[String]) = {
    val output ="output/wordcount"
    val sc = new SparkContext("local", "WordCount", new SparkConf().setAppName(" Word Count"))
    try {
      val input = sc.textFile("hdfs://localhost:9000/user/dpanw/input/data1.csv").map(line => line.toUpperCase())
      val result = input.flatMap(line=>line.split(","))
        .map(word=>(word,1))
        .reduceByKey(_+_)
            result.saveAsTextFile("hdfs://localhost:9000/user/dpanw/output/result.csv")
      println("Word Count Problem")
    } finally {
      sc.stop()
    }
  }
  }