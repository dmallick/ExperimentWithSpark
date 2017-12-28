package dev.spark.analytics

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession

object WindowsExample {
  def main(args: Array[String]) = {

    val windowDuration = "10 seconds"
    val slideDuration = "5 seconds"

    val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulNetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(7))
    // Set checkpoint directory
    ssc.checkpoint(".")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    //val lines = ssc.socketTextStream("localhost", 9999)

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCountWindowed")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    val windowedCounts = words.groupBy(window($"timestamp", windowDuration, slideDuration), $"word").count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }

}
