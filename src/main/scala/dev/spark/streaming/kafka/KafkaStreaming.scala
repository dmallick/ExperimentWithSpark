package dev.spark.streaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming {
  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //START for Spark1.6.2
    /*val topicsSet = "HelloKafkaTopic".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).countByValue()

    wordCounts.print()
    println("|---------|")
    wordCounts.print()
    println("$---------$")*/

    //END for Spark1.6.2

    val topicsSet = "HelloKafkaTopic".split(",").toSet
    //val kafkaParams = Map[String, String]("bootstrap.servers" -> "localhost:9092", "value.deserializer" -> classOf[StringDeserializer])
    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092","key.deserializer" -> classOf[StringDeserializer],"value.deserializer" -> classOf[StringDeserializer],"group.id" -> "cg-1")
    val messages = KafkaUtils.createDirectStream[String, String]( ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    wordCounts.print()
    println("|---------|")
    wordCounts.print()
    println("$---------$")
    ssc.start()
    ssc.awaitTermination()
  }
}
