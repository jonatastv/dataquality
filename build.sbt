name := "dataquality-master"

version := "1.1"

scalaVersion := "2.11.2"

//idePackagePrefix := Some("br.com.vivo.dataquality")

val sparkGroup = "org.apache.spark"

scalaVersion := "2.11.2"

val spark_version = "2.2.0"

libraryDependencies ++= Seq(
  // SPARK
  sparkGroup %% "spark-core" % spark_version,
  sparkGroup %% "spark-sql" % spark_version,
 // "org.apache.hbase" % "hbase-client" % "1.1.2",
 // "org.apache.hbase.connectors.spark" % "hbase-spark" % "1.0.0",
  //"org.apache.hbase" % "hbase-common" % "2.0.0"
  "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"


  /*"org.apache.hbase" % "hbase-server" % "1.2.1",
  "org.apache.hbase" % "hbase-client" % "1.2.1",
  "org.apache.hbase" % "hbase-common" % "1.2.1",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3"
*/
)
libraryDependencies +=  "org.apache.hbase" % "hbase-common" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"), ExclusionRule(organization = "org.codehaus.jackson", name="jackson-core-asl"))

libraryDependencies +=  "org.apache.hbase" % "hbase-client" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"), ExclusionRule(organization = "org.codehaus.jackson", name="jackson-core-asl"))

libraryDependencies +=  "org.apache.hbase" % "hbase-server" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"), ExclusionRule(organization = "org.codehaus.jackson", name="jackson-core-asl"))



