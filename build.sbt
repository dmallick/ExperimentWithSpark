name := "ExperimentWithSpark"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

//For spark1.6.2 with scala 2.10.6
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.2"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.2"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
