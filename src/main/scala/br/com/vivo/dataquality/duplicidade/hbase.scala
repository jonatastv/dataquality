package br.com.vivo.dataquality.duplicidade


import org.apache.spark.sql.{DataFrame, SparkSession}
import it.nerdammer.spark.hbase._
import org.apache.spark.{SparkConf, SparkContext}


object hbase extends App {



  val sparkConf = new SparkConf()
  sparkConf.set("spark.hbase.host", "thehost")
  val sc = new SparkContext(sparkConf)

  /*
  val spark: SparkSession = SparkSession.builder()
   // .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
*/
  val hBaseRDD = sc.hbaseTable[(String, Int, String)]("TERADATA_VIVOPLAY:VW_API_PPV_CMPR_VIVO_PLAY_V2")
    .select("canal", "codigo_unico_transacao")
    .inColumnFamily("vw_api_ppv_cmpr_vivo_play_cf")

  hBaseRDD.foreach(t => {
    if(t._1.nonEmpty) println(t._1.indices)
  })



/*
  def catalog =
    s"""{
       |"table":{"namespace":"TERADATA_VIVOPLAY", "name":"VW_API_PPV_CMPR_VIVO_PLAY_V2"},
       |"rowkey":"key",
       |"columns":{
       |"key":{"vw_api_ppv_cmpr_vivo_play_cf":"canal", "col":"key", "type":"string"},
       |"fName":{"vw_api_ppv_cmpr_vivo_play_cf":"codigo_unico_transacao", "col":"firstName", "type":"string"}
       |}
       |}""".stripMargin

  /*

  time spark-submit --master yarn  \
    --queue Qualidade \
    --class br.com.vivo.dataquality.duplicidade.hbase \
   /home/SPDQ/indra/dataquality-master_2.11.jar
   */
/*  val spark = SparkSession
    .builder()
    .appName("Duplicidade")
    .config("spark.sql.broadcastTimeout", "36000")
    //.enableHiveSupport()
    .getOrCreate()
*/
  import spark.implicits._

  val hbaseDF = spark.read
    .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  hbaseDF.printSchema()

  hbaseDF.show(false)
*/
}
