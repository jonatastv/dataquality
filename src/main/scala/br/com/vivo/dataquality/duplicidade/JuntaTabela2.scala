package br.com.vivo.dataquality.duplicidade


import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter, StringWriter}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object JuntaTabela2 extends  App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val sciencedatabase : String = args(3)
  val nome_tabela_tmp: String = args(4)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)
  Logger.getLogger("com.hortonworks").setLevel(Level.OFF)
  Logger.getLogger("com.qubole").setLevel(Level.OFF)


  /**
  | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.duplicidade.JuntaTabela \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money 20210520
   | dq_duplicados_medidas_aux_01_coletaDuplicidade_p_bigd_urm_tbgd_turm_controle_faturas_vivo_money_t

   */

    try{

  val spark = SparkSession
    .builder()
    .appName(s"juntaTabela2_$table")
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

  val erroutput = new StringWriter

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

    hive.dropTable(s"h_bigd_dq_db.dq_d_m_${nome_tabela_tmp}", true, false)

    val tabela = hive.executeQuery(
      s"""
         |select
         |--'${database}'  x1,
         |--'${table}' x2,
         |--B.dt_foto x3,
         |--B.dt_processamento x4,
         |---
         |A.banco,
         |A.tabela,
         |A.dt_foto,
         |A.dt_processamento,
         |A.qtde1,
         |A.qtde2,
         |A.diferenca,
         |'1' as fonte
         |
         |from h_bigd_dq_db.dq_duplicados_medidas${projeto(i)} A
         |left join h_bigd_dq_db.${nome_tabela_tmp} B
         |   on B.banco = A.banco
         |   and B.tabela = B.tabela
         |where
         |-- FILTRO DO BANCO + TABELA + DT_PROCESSAMENTO
         |concat (
         |   trim(nvl(A.banco,'')), '|',
         |   trim(nvl(A.tabela,'')), '|',
         |   trim(nvl(A.dt_foto,'')), '|',
         |   trim(nvl(A.dt_processamento,''))
         |)
         |   <>
         |concat (
         |   trim(nvl('${database}','')), '|',
         |   trim(nvl('${table}','')), '|',
         |   trim(nvl(B.dt_foto,'')), '|',
         |   trim(nvl(B.dt_processamento,''))
         |
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
         |A.qtde1,
         |A.qtde2,
         |A.diferenca,
         |'2' as fonte
         |from h_bigd_dq_db.${nome_tabela_tmp} A
         |
         |order by dt_foto
         |""".stripMargin)


    val temp=s"h_bigd_dq_db.dq_d_m_${nome_tabela_tmp}"

    tabela.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",temp).save()
    hive.dropTable(s"h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}", true, false)
    hive.executeUpdate(s"alter table h_bigd_dq_db.dq_d_m_${nome_tabela_tmp} rename to h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}")

    hive.dropTable(s"h_bigd_dq_db.dq_d_m_${nome_tabela_tmp}", true, false)

//    val cretedf = spark.sql(
//      s"""
//         |insert overwrite table h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}
//         |select distinct * from temp
//         |""".stripMargin)

    log.info(s"INSERT OVERWRITE TABLE h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}")
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
