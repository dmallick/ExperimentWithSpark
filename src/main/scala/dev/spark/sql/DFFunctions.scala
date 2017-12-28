package dev.spark.sql

import org.apache.spark.sql.SparkSession

object DFFunctions {
  case class Car(Model: String ,MPG: Double, Cylinders: Int, EngineDisp: Double,Horsepower: Double,Weight: Double,Accelerate: Double,Year: Int,Origin: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]").config("spark.some.config.option", "some-value").getOrCreate()
    //import spark.implicits._
    /*val carDF = spark.sparkContext.textFile("/appl/software/input/cars.csv").map(_.split(","))
      .map(attributes => Car(
        attributes(0).toString,
        attributes(1).toFloat,
        attributes(2).trim().toInt,
        attributes(3).trim.toDouble,
        attributes(4).trim().toDouble,
        attributes(5).trim.toDouble,
        attributes(6).trim().toDouble,
        attributes(7).trim().toInt,
        attributes(8).trim())).toDF();*/

    //val carDF = spark.read.csv("/appl/software/input/cars.csv").rdd.map(_.split(","))
    val carDF = spark.read.option("header", true).option("inferSchema", "true").csv("/appl/software/input/cars.csv").toDF("Model","MPG","Cylinders","EngineDisp","Horsepower","Weight","Accelerate","Year","Origin")
    carDF.printSchema()
    carDF.createOrReplaceTempView("cars")
    val modelDF = spark.sql("SELECT Model FROM cars").show()

  }
}