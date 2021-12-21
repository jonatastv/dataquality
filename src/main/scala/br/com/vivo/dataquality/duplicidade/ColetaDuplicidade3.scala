package br.com.vivo.dataquality.duplicidade

import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object ColetaDuplicidade3 extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_nome_campo: String = args(2)
  val var_data_foto: String = args(3)
  val nome_tabela_tmp: String = args(4)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)
  Logger.getLogger("com.hortonworks").setLevel(Level.OFF)
  Logger.getLogger("com.qubole").setLevel(Level.OFF)

  try{

  val spark = SparkSession
    .builder()
    .appName(s"Duplicidade_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

    val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()
    spark.conf.set("hive.tez.queue.name","Qualidade")
    spark.conf.set("mapreduce.map.memory","5120")
    spark.conf.set("mapreduce.reduce.memory","5120")

  log.info(s"Iniciando aplicação spark")

  val applicationId: String = spark.sparkContext.applicationId

  log.info(s"**********************************************************************************")
  log.info(s"*** Application ID: $applicationId")
  log.info(s"**********************************************************************************")

    hive.dropTable(s"h_bigd_dq_db.${nome_tabela_tmp}", true, false)
//  val dropDF = spark.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t")
  log.info(s"drop table if exists h_bigd_dq_db.${nome_tabela_tmp}")

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
  log.info("Executando query")

  val duplicateDF = hive.executeQuery(
    s"""
 -- create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t
 -- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as

select
A2.banco,
A2.tabela,
A2.dt_foto,
A2.dt_processamento,
cast(B2.qtde1 as bigint) qtde1,
cast(C2.qtde2 as bigint) qtde2,
cast(B2.qtde1 as bigint) - cast(C2.qtde2 as bigint) diferenca

from (
   -- GRUPO 1
   select
   '${database}' as banco,
   '${table}' as tabela,
   date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento
) as A2

left join (
   select
   date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
   count(B1.${var_nome_campo}) as qtde1
   from ${database}.${table} as B1

) as B2
   on B2.dt_foto = A2.dt_foto

left join (
   select
   date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
   count(C1.${var_nome_campo}) as qtde2
   from (
      select distinct * from ${database}.${table}
   ) as C1
) as C2
   on C2.dt_foto = A2.dt_foto
    """)

    hive.setDatabase(s"h_bigd_dq_db")
    hive.createTable(s"${nome_tabela_tmp}").ifNotExists()
      .column("banco", "string")
      .column("tabela", "string")
      .column("dt_foto", "string")
      .column("dt_processamento", "string")
      .column("qtde1", "bigint")
      .column("qtde2", "bigint")
      .column("diferenca", "bigint")
      .create()

  log.info(s"salvando na tabela h_bigd_dq_db.${nome_tabela_tmp}")

    val tablename = s"h_bigd_dq_db.${nome_tabela_tmp}"
    duplicateDF.write.mode("append").format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",tablename).save()

    log.info(s"Processo transformação do Hive completo")


  } catch {
    case  exception: AuthenticationException =>
      log.error("Falha ao conectar no Hive.")
      log.error(s"Tipo de Falha => ${exception.getStackTrace}")
      log.error(exception.getMessage)
      log.error(exception)

    case exception: AnalysisException =>
      log.error("Falha na execução da Query.")
      log.error(s"Tipo de Falha => ${exception.getStackTrace}")
      log.error(exception.getMessage)
      log.error(exception)

    case e: ClassCastException =>
      log.error("Falha com os tipos dos dados da tabela.")
      log.error(s"Tipo de Falha => ${e.getStackTrace}")
      log.error(e.getMessage)
      log.error(e)

    case e: Exception =>
      log.error("Falha Genérica.")
      log.error(s"Tipo de Falha => ${e.getStackTrace}")
      log.error(e.getMessage)
      log.error(e)

  }

}
