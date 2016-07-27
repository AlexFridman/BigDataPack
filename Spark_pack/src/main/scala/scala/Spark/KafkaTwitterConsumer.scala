package scala.Spark

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
/**
 * This is a kafka consumer , which is consuming twitter streaming data from producer
 * before storing data is compressed using snappy compression and stored in parquet file
 *
 * command  : spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 --class scala.Spark.KafkaWordCount --master local[4] /opt/Spark-0.0.1-SNAPSHOT.jar localhost:2181 group twittertopic 2
 * here 2 is a  number of thread at last of command
 */
object KafkaWordCount {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum><group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.set("spark.driver.allowMultipleContexts", "true");

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val sparkContext = new SparkContext("local", "Kafka", sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
  //  sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) //creates key value and emits only value using map(_._2)
    val test = lines.window(Minutes(10), Seconds(10))
    test.repartition(2)
    var i = 0;
    test.foreachRDD { x =>
      x.map(p => Record(p)).toDF().write.parquet("hdfs://localhost:9000/user/dpanw/kafka1/file" + i)
      i += 1
    }
    i = i + 1
    // test.saveAsTextFiles("/opt/KafkaOutput/data") //hdfs://localhost:9000/user/dpanw/kafka
    ssc.start()
    ssc.awaitTermination()
  }
  case class Record(word: String)

}