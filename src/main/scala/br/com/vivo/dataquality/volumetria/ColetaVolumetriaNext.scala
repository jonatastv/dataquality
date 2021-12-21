package br.com.vivo.dataquality.volumetria

import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveContext
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object ColetaVolumetriaNext extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)
  val var_formato_dt_foto: String = args(4)
  val nome_tabela_tmp: String = args(5)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)

  try{

  /**
  | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.volumetria.ColetaVolumetria  \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money  20210520 dt_foto 1
   */

  val spark = SparkSession
    .builder()
    .appName(s"Volumetria_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

  log.info(s"Iniciando aplicação spark")

    val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()
    spark.conf.set("hive.tez.queue.name","Qualidade")
    spark.conf.set("mapreduce.map.memory","5120")
    spark.conf.set("mapreduce.reduce.memory","5120")

  val applicationId: String = spark.sparkContext.applicationId

  log.info(s"**********************************************************************************")
  log.info(s"*** Application ID: $applicationId")
  log.info(s"**********************************************************************************")


  val parametrosDf = spark.sql(
    s"""
       |select disponibilidade_fonte ,
       |disponibilidade_detalhe ,
       |tabela_medida
       |from h_bigd_dq_db.dq_parametros
       |where tabela = '${table}'
       |""".stripMargin)

  log.info(s"lendo tabela h_bigd_dq_db.dq_parametros")
  log.info(s"${parametrosDf.show(1)}")

  if(parametrosDf.count() == 0){
    log.info(s"Não existe '${table}' na tabela h_bigd_dq_db.dq_parametros")
  }

  val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
    projeto_2.getString(0).toLowerCase
  }

  for( i <-  projeto.indices ) {

    log.info("executando query")

    hive.dropTable(s"h_bigd_dq_db.${nome_tabela_tmp}", true, false)

    val tabela = hive.executeQuery(
      s"""
         |select
         |'${database}' as banco,
         |'${table}' as tabela,
         |'${var_data_foto}' as dt_foto,
         |date_format(current_date(),"yyyyMMdd") as dt_processamento,
         |A2.qtde_registros,
         |'2' as fonte
         |from (
         |select count(*) as qtde_registros from ${database}.${table}
         |where p_state = 'published'
         |and   p_workflow_id = 'event-compacted'
         |and   p_attempt = '0'
         |and
         |    case
         |       when '$var_formato_dt_foto' = '1' then cast($var_nome_campo as string) = '$var_data_foto'
         |       when '$var_formato_dt_foto' = '2' then cast(date_format($var_nome_campo,"yyyyMMdd") as string) = '$var_data_foto'
         |    end
         | ) as A2
         |""".stripMargin).toDF()

    hive.setDatabase(s"h_bigd_dq_db")
    hive.createTable(s"${nome_tabela_tmp}").ifNotExists()
      .column("banco", "string")
      .column("tabela", "string")
      .column("dt_foto", "string")
      .column("dt_processamento", "string")
      .column("qtde_registros", "bigint")
      .column("fonte", "string")
      .create()


    log.info(s"salvando tabela h_bigd_dq_db.${nome_tabela_tmp}")

    val tablename = s"h_bigd_dq_db.${nome_tabela_tmp}"
    tabela.write.mode("append").format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",tablename).save()

    log.info(s"Processo transformação do Hive completo")

    //  .where(col("A.tabela") === "${table}")
    //.limit(5)

  }

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