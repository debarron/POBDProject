name := "Scala-SparkQueryProcessing"

version := "1.0"

scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1"
libraryDependencies += "com.github.spirom" %% "spark-mongodb-connector" % "0.5.3"



