package br.com.vivo.dataquality.volumetria

import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object JuntaTabelaVolumetria extends  App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  //val var_tabela_auxiliar: String = args(3)
  val sciencedatabase : String = args(3)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
//  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)
  Logger.getLogger("com.hortonworks").setLevel(Level.OFF)
  Logger.getLogger("com.qubole").setLevel(Level.OFF)


    try{



  val spark = SparkSession
    .builder()
    .appName(s"junta_T_V_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.port.maxRetries", "100")
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

  if(parametrosDf.count() == 0){
    log.info(s"Não existe '${table}' na tabela h_bigd_dq_db.dq_parametros")
  }

  val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
    projeto_2.getString(0).toLowerCase
  }

  for( i <-  projeto.indices ) {

    log.info("Executando query -- HISTORICO DO CUBO")

//     val dropTabelaAux = spark.sql(s"drop table if exists h_bigd_dq_db.dq_volumetria_medidas_${database}_${table}_t")
    hive.dropTable(s"h_bigd_dq_db.dq_v_m_${table}_${var_data_foto}_t", true, false)
    val tabela = hive.executeQuery(
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
         |A.qtde_registros,
         |'2' as fonte
         |from h_bigd_dq_db.dq_volumetria_medidas_${database}_${table}_t A
         |order by dt_foto
         |""".stripMargin).toDF()

    val temp=s"h_bigd_dq_db.dq_v_m_${table}_${var_data_foto}_t"

    tabela.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",temp).save()

    val tablename = s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}"
    hive.dropTable(s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}", true, false)
    hive.executeUpdate(s"alter table h_bigd_dq_db.dq_v_m_${table}_${var_data_foto}_t rename to h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}")

    hive.dropTable(s"h_bigd_dq_db.dq_v_m_${table}_${var_data_foto}_t", true, false)

    log.info(s"alter table h_bigd_dq_db.dq_v_m_${table}_${var_data_foto}_t rename to h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}")
    log.info(s"Processo transformação do Hive completo")

//    log.info(s"INSERT OVERWRITE TABLE h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}")
//    log.info(s"Processo transformação do Hive completo")


/*


    val dfs = spark.sql(s"select qtde_registros from h_bigd_dq_db.dq_volumetria_medidas_${database}_${table}_t")



    val qtde_registros: Array[Long] = for (total <- dfs.select("qtde_registros").collect()) yield {
      total.getLong(0)
    }


    for( f <-  qtde_registros.indices ) {


      if(qtde_registros(f) > 0){

        log.info("iniciando limpeza da tabela dq_volumetria_medidas")

        val volumetria_corrigido = spark.sql(
          s"""
             |select distinct banco, tabela, dt_foto, var_nome_campo, var_formato_dt_foto , 1 as status
             |from h_bigd_dq_db.dq_volumetria_falhas
             | where tabela = '${table}'
             | and dt_foto = '${var_data_foto}'
             | and status = 0
             |limit 1
             |""".stripMargin)

        val volumetria = spark.sql(
          s"""
             |select  *
             |from h_bigd_dq_db.dq_volumetria_falhas
             |where
             |tabela not in ('tbgd_turm_customer')
             |""".stripMargin)

        val dq_volumetria_falhas = volumetria.select("banco","tabela","dt_foto","var_nome_campo","var_formato_dt_foto","status")
          .where(s"""concat(banco, tabela, dt_foto) <> concat('${database(i)}','${table(i)}','${var_data_foto(i)}')""")
          .unionAll(
            volumetria_corrigido.select(
              volumetria_corrigido.col("banco"),
              volumetria_corrigido.col("tabela"),
              volumetria_corrigido.col("dt_foto"),
              volumetria_corrigido.col("var_nome_campo"),
              volumetria_corrigido.col("var_formato_dt_foto"),
              volumetria_corrigido.col("status")
            )
          )
          .dropDuplicates
          .orderBy("dt_foto")


        dq_volumetria_falhas.registerTempTable("dq_volumetria_falhas_temp")

        val droprow = spark.sql(
          s"""
          insert overwrite table h_bigd_dq_db.dq_volumetria_falhas
          select distinct * from dq_volumetria_falhas_temp
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
