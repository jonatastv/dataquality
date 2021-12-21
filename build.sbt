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
  //"com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.0.0.0-1574"


  //"com.hortonworks.hive" % "hive-warehouse-connector_2.11" % "1.0.0.7.2.9.0-203" % "provided"


  /*"org.apache.hbase" % "hbase-server" % "1.2.1",
  "org.apache.hbase" % "hbase-client" % "1.2.1",
  "org.apache.hbase" % "hbase-common" % "1.2.1",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3"
*/
)
libraryDependencies +=  "org.apache.hbase" % "hbase-common" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"), ExclusionRule(organization = "org.codehaus.jackson", name="jackson-core-asl"))

libraryDependencies +=  "org.apache.hbase" % "hbase-client" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"), ExclusionRule(organization = "org.codehaus.jackson", name="jackson-core-asl"))

libraryDependencies +=  "org.apache.hbase" % "hbase-server" % "1.0.3" excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty", name="jetty"), ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"), ExclusionRule(organization = "org.codehaus.jackson", name="jackson-core-asl"))

// https://mvnrepository.com/artifact/com.qubole/spark-acid
//libraryDependencies += "com.qubole" %% "spark-acid" % "0.4.4.7.2.7.7-2"

//resolvers += "Cloudera repo" at "https://repository.cloudera.com/artifactory/cloudera-repos"
//libraryDependencies += "com.hortonworks.hive" % "hive-warehouse-connector_2.11" % "1.0.0.7.2.9.0-203" % "provided"
//resolvers += "Hortonworks repo" at "http://repo.hortonworks.com/content/repositories/releases/"
//libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.1.0.0-78"

//libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.0.0.4-42"

//libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector_2.11" % "1.0.0.3.0.0.4-42" from "file:///C:\\Users\\jtvieira\\Downloads\\hive-warehouse-connector_2.11-1.0.0.3.0.0.4-42.jar"

libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector-assembly" % "1.0.0.7.1.6.0-297" from "file:///C:\\Users\\jtvieira\\Downloads\\hive-warehouse-connector-assembly-1.0.0.7.1.6.0-297.jar"