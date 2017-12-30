/*
* Writing DF to DB tables
*
* */
package dev.spark.db

import java.util.Properties

import org.apache.spark.sql.SparkSession

object WriteDFToDB {

  def main(args: Array[String]) {

    val dataPath = "/appl/software/input/diamonds.csv"
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]").config("spark.some.config.option", "some-value").getOrCreate()
    val diamondDF=spark.read.option("header","true").option("inferSchema", "true").csv(dataPath)

    diamondDF.show(2)
    val jdbcUsername = "root"
    val jdbcPassword = "cloudera"
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase ="employees"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "cloudera")

    diamondDF.write.mode("append").jdbc(jdbcUrl, "diamond", connectionProperties)
  }
}
