package br.com.vivo.dataquality.duplicidade

import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

import java.io.{File, PrintWriter, StringWriter}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object JuntaTabela extends  App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  //val var_tabela_auxiliar: String = args(3)
  val sciencedatabase : String = args(3)

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
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money 20210520 dq_duplicados_medidas_aux_01_coletaDuplicidade_p_bigd_urm_tbgd_turm_controle_faturas_vivo_money_teste

*/


  try{


  val spark = SparkSession
    .builder()
    .appName(s"junta_T_D_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.port.maxRetries", "100")
    .enableHiveSupport()
    .getOrCreate()

  log.info(s"Iniciando aplicação spark")
    spark.conf.set("hive.tez.queue.name","Qualidade")
    spark.conf.set("mapreduce.map.memory","5120")
    spark.conf.set("mapreduce.reduce.memory","5120")

    val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()

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

    // val dropTabelaAux = spark.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}")
    hive.dropTable(s"h_bigd_dq_db.dq_d_m_${table}_${var_data_foto}", true, false)

    val tabela = hive.executeQuery(
      s"""
         |-- create Table h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}
         |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
         |
         |-- HISTORICO DO CUBO
         |select
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
         |   '${var_data_foto}',
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
         |A.qtde1,
         |A.qtde2,
         |A.diferenca,
         |'2' as fonte
         |from h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto} A
         |order by dt_foto
         |""".stripMargin)

    val temp=s"h_bigd_dq_db.dq_d_m_${table}_${var_data_foto}"

    tabela.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",temp).save()
    hive.dropTable(s"h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}", true, false)
    hive.executeUpdate(s"alter table h_bigd_dq_db.dq_d_m_${table}_${var_data_foto} rename to h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}")

    hive.dropTable(s"h_bigd_dq_db.dq_d_m_${table}_${var_data_foto}", true, false)

//    tabela.write.mode("overwrite").

//    hive.executeUpdate(
//      s"""
//         |insert overwrite table h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}
//         |select * from h_bigd_dq_db.temp_${table}_${var_data_foto}
//         |""".stripMargin)


//    val tablename = s"h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}"
//    tabela.write.mode("overwrite").format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",tablename).save()

    log.info(s"alter table h_bigd_dq_db.dq_d_m_${table}_${var_data_foto} rename to h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}")
    log.info(s"Processo transformação do Hive completo")


/*


    val dfs = spark.sql(s"select qtde1 from h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}")


    val qtde1: Array[Long] = for (teste <- dfs.select("qtde1").collect()) yield {
      teste.getLong(0)
    }


    for( f <-  qtde1.indices ) {


      if( qtde1(f) > 0){

        log.info("iniciando limpeza da tabela dq_duplicidade_falhas")

    val duplicidade_corrigido = spark.sql(
      s"""
         |select distinct banco, tabela, dt_foto, var_nome_campo, var_formato_dt_foto , 1 as status
         |from h_bigd_dq_db.dq_duplicidade_falhas
         | where tabela = '${table}'
         | and dt_foto = '${var_data_foto}'
         | and status = 0
         |limit 1
         |""".stripMargin)

    val duplicidade = spark.sql(
      s"""
         |select  *
         |from h_bigd_dq_db.dq_duplicidade_falhas
         |where
         |tabela not in ('tbgd_turm_customer')
         |""".stripMargin)

    val dq_duplicidade_falhas = duplicidade.select("banco","tabela","dt_foto","var_nome_campo","var_formato_dt_foto","status")
      .where(s"""concat(banco, tabela, dt_foto) <> concat('${database(i)}','${table(i)}','${var_data_foto(i)}')""")
      .unionAll(
        duplicidade_corrigido.select(
          duplicidade_corrigido.col("banco"),
          duplicidade_corrigido.col("tabela"),
          duplicidade_corrigido.col("dt_foto"),
          duplicidade_corrigido.col("var_nome_campo"),
          duplicidade_corrigido.col("var_formato_dt_foto"),
          duplicidade_corrigido.col("status")
        )
      )
      .dropDuplicates
      .orderBy("dt_foto")


    dq_duplicidade_falhas.registerTempTable("dq_duplicidade_falhas_temp")

    val droprow = spark.sql(
      s"""
          insert overwrite table h_bigd_dq_db.dq_duplicidade_falhas
          select distinct * from dq_duplicidade_falhas_temp
         """)

      }

    }
*/

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
