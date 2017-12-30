package dev.spark.db

import java.util.Properties
import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
object DFDBFunctions {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]").config("spark.some.config.option", "some-value").getOrCreate()
    val jdbcUsername = "root"
    val jdbcPassword = "cloudera"
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase ="employees"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "cloudera")

    val employees_table = spark.read.jdbc(jdbcUrl, "employees", connectionProperties)
    employees_table.show(2)
 
  }
}
