package br.com.vivo.dataquality.volumetria

import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object JuntaTabelaVolumetria2 extends App {


  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val sciencedatabase : String = args(3)
  val nome_tabela_tmp: String = args(5)

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
      .appName(s"juntaTabela_$table")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.port.maxRetries", "100")
      .enableHiveSupport()
      .getOrCreate()

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

    if(parametrosDf.count() == 0){
      log.info(s"Não existe '${table}' na tabela h_bigd_dq_db.dq_parametros")
    }

    val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
      projeto_2.getString(0).toLowerCase
    }

    for( i <-  projeto.indices ) {

      log.info("Executando query -- HISTORICO DO CUBO")

      hive.dropTable(s"h_bigd_dq_db.dq_v_m_${nome_tabela_tmp}", true, false)

      val tabela = spark.sql(
        s"""
           |-- create Table h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}
           |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
           |
           |-- HISTORICO DO CUBO
           |select
           |A.banco,
           |A.tabela,
           |A.dt_foto,
           |A.dt_processamento,
           |A.qtde_registros,
           |'1' as fonte
           |
           |from h_bigd_dq_db.dq_volumetria_medidas${projeto(i)} A
           |where
           |-- FILTRO DO BANCO + TABELA + DT_FOTO + DT_PROCESSAMENTO
           |concat (
           |   A.banco,
           |   A.tabela,
           |   A.dt_foto,
           |   A.dt_processamento
           |)
           |   <>
           |concat (
           |   '${database}',
           |   '${table}',
           |   date_format(date_sub(current_date() , 1 ),"yyyyMMdd"),
           |   date_format(current_date(),"yyyyMMdd")
           |)
           |
           |----------
           |union all
           |----------
           |
           |select
           |A.banco,
           |A.tabela,
           |A.dt_foto,
           |A.dt_processamento,
           |A.qtde_registros,
           |'2' as fonte
           |from h_bigd_dq_db.${nome_tabela_tmp} A
           |order by dt_foto
           |""".stripMargin).toDF()



      val temp=s"h_bigd_dq_db.dq_v_m_${nome_tabela_tmp}"

      tabela.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",temp).save()

      val tablename = s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}"
      hive.dropTable(s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}", true, false)
      hive.executeUpdate(s"alter table h_bigd_dq_db.dq_v_m_${nome_tabela_tmp} rename to h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}")

      hive.dropTable(s"h_bigd_dq_db.dq_v_m_${nome_tabela_tmp}", true, false)
      log.info(s"Processo transformação do Hive completo")

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
