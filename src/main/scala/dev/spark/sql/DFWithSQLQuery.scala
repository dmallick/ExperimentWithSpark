package dev.spark.sql

import org.apache.spark.sql.SparkSession

object DFWithSQLQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]").config("spark.some.config.option", "some-value").getOrCreate()
    val carDF = spark.read.option("header", true).option("inferSchema", "true").csv("/appl/software/input/cars.csv").toDF("Model","MPG","Cylinders","EngineDisp","Horsepower","Weight","Accelerate","Year","Origin")
    import spark.implicits._
    import org.apache.spark.sql._
    import org.apache.spark.sql.functions._

    //getting SQL Context from Spark Session
    var sqlContext = spark.sqlContext

    //Finding max, min, avg
    carDF.select(max($"Weight"), min($"Weight"), avg($"Weight")).show()

    // Use case: http://datascience.stackexchange.com/questions/6546/how-to-calculate-the-mean-of-a-dataframe-column-and-find-the-top-10
    //Standard Deviation in the spark way
    //carDF.groupBy($"Cylinders").agg(mySd($"Weight").alias("tem_sd")).show()

    //Standard Deviation in DataFrame way
    /*carDF.registerTempTable("df")
      sqlContext.sql("""SELECT Cylinders, stddev(Weight) FROM df GROUP BY Cylinders""").show()*/

    //Standard Deviation in DataFrame way
    /*import org.apache.spark.sql.functions.{stddev_samp, stddev_pop}
      carDF.groupBy($"Cylinders").agg(stddev_pop($"Weight")).show()*/

    // Cross Tabulation
    //users will be able to cross-tabulate two columns of a DataFrame in order to obtain the counts of the different pairs that are observed in those columns. Here is an example on how to use crosstab to obtain the contingency table.
    //https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
    // carDF.stat.crosstab("Cylinders", "EngineDisp").show()

    // Frequent Items: Figuring out which items are frequent in each column
    //carDF.stat.freqItems(Seq("Cylinders", "Weight")).show()

    //To get summary statistics on numerical fields, call "describe":
    //carDF.describe("Cylinders").show()

    //Filter: To filter for rows that satisfy a given predicate:
    //carDF.filter($"Weight" > 2385).show()

    //Map: To map over rows with a given lambda function:
    //studentDS.map(student=> student)

    /*val studentDF = spark.read.json("D:\\Debasish\\BigData\\Input\\students.json");
    case class Student(name: String, dept: String, age: Long)
    val studentDS = studentDF.as("Student")
    //studentDS.show()
    case class Department(name: String, building: Int)
    val depts = Seq(Department("Math", 125), Department("CS", 110), Department).asInstanceOf[Department]
    val depts = Seq(("Math", 125), ("CS", 110))*/

    //http://xinhstechblog.blogspot.co.uk/2016/07/overview-of-spark-20-dataset-dataframe.html

    //val df = carDF.select(carDF("Origin"),carDF("Horsepower").cast(IntegerType))
    //df.groupBy($"Origin").max("Horsepower").as("Horsepower").sort("max(Horsepower)").show


    //df.withColumn("year2", 'year.cast("Int")).select('year2 as 'year, 'make, 'model, 'comment, 'blank)

    //using with util method to do the conversion
    /*      import com.spark.sql.util.DFHelper._
          val df = castColumnTo( carDF, "Horsepower", IntegerType )
          df.groupBy($"Origin").max("Horsepower").as("Horsepower").sort("max(Horsepower)").show*/

    //return the column values as single list:
    //carDF.select($"Horsepower").rdd.map(r => r(0)).collect().map(x=> println(x))
    /*    +--------+---------------+
          |  Origin|max(Horsepower)|
          +--------+---------------+
          |Japanese|            132|
          |European|            133|
          |American|            230|
          +--------+---------------+
    */



  }

}
