package dev.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming {
  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    val topicsSet = "HelloKafkaTopic".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).countByValue()
    println("|---------|")
    wordCounts.print()
    println("$---------$")
    ssc.start()
    ssc.awaitTermination()
  }
}
