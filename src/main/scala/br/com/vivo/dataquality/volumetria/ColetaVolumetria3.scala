package br.com.vivo.dataquality.volumetria

import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object ColetaVolumetria3 extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val sciencedatabase : String = args(3)
  val nome_tabela_tmp: String = args(4)


  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  log.info(s"database origem :$database")
  log.info(s"tabela :$table")
  log.info(s"database destino: $sciencedatabase")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)
  Logger.getLogger("com.hortonworks").setLevel(Level.OFF)
  Logger.getLogger("com.qubole").setLevel(Level.OFF)

  try{



  val spark = SparkSession
    .builder()
    .appName(s"Volumetria3_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.port.maxRetries", "100")
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



  val parametrosDf = spark.sql(
    s"""
       |select disponibilidade_fonte ,
       |disponibilidade_detalhe ,
       |tabela_medida
       |from $sciencedatabase.dq_parametros
       |where tabela = '${table}'
       |""".stripMargin)

  log.info(s"lendo tabela $sciencedatabase.dq_parametros")
  log.info(s"${parametrosDf.show(1)}")

    if(parametrosDf.count() == 0){
      log.info(s"Não existe '${table}' na tabela $sciencedatabase.dq_parametros")
    }

  val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
    projeto_2.getString(0).toLowerCase
  }

  for( i <-  projeto.indices ) {


    hive.dropTable(s"h_bigd_dq_db.${nome_tabela_tmp}", true, false)
    log.info(s"drop table if exists h_bigd_dq_db.${nome_tabela_tmp}")

    log.info("Executando query")

  val tabela = hive.executeQuery(
    s"""
       |-- create Table IF NOT EXISTS $sciencedatabase.dq_volumetria_medidas_${database}_${table}_t
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |select
       |'${database}' as banco,
       |'${table}' as tabela,
       |date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
       |date_format(current_date(),"yyyyMMdd") as dt_processamento,
       |A2.qtde_registros,
       |'2' as fonte
       |from (
       |select count(*) as qtde_registros from ${database}.${table}
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

   // log.info(s"${tabela.show(1)}")
/*
  val volumetria_medidas = spark.table(s"$sciencedatabase.dq_volumetria_medidas${projeto(i)}").as("A")
    .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
    .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}',date_format(date_sub(current_date() , 1 ),"yyyyMMdd"), date_format(current_date(),"yyyyMMdd") )""")
    .withColumn("fonte",lit(1))
    .unionAll(
      tabela.select(
        tabela.col("banco"),
        tabela.col("tabela"),
        tabela.col("dt_foto"),
        tabela.col("dt_processamento"),
        tabela.col("qtde_registros"),
        tabela.col("fonte")
      )
    )
    .dropDuplicates
    .orderBy("dt_foto")


  volumetria_medidas.createOrReplaceTempView("volumetria_medidas")
  val final_medidas = spark.sql(
    s"""
       |-- create Table IF NOT EXISTS $sciencedatabase.dq_volumetria_medidas${projeto(i)}
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table $sciencedatabase.dq_volumetria_medidas${projeto(i)}
       |select  * from volumetria_medidas""".stripMargin)
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


