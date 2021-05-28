name := "dataquality"

version := "0.1"

scalaVersion := "2.10.6"

//idePackagePrefix := Some("br.com.vivo.dataquality")

val sparkGroup = "org.apache.spark"

scalaVersion := "2.10.6"

val spark_version = "1.6.3"

libraryDependencies ++= Seq(
  // SPARK
  sparkGroup %% "spark-core" % spark_version,
  sparkGroup %% "spark-sql" % spark_version ,
  sparkGroup %% "spark-hive" % "1.5.0"

)

