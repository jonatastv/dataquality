package br.com.vivo.dataquality.volumetria


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.http.auth.AuthenticationException

object ColetaVolumetriaInvoicing extends  App {

  val database: String = args(0)
  val table: String = args(1)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)

  try{

  val spark = SparkSession
    .builder()
    .appName(s"Volumetria_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

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
       |from h_bigd_dq_db.dq_parametros
       |where tabela = '${table}'
       |""".stripMargin)


  val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
    projeto_2.getString(0).toLowerCase
  }

  for( i <-  projeto.indices ) {


  val tabela = spark.sql(
    s"""
       |select
       |'${database}' as banco,
       |'${table}' as tabela,
       |A2.dt_foto,
       |date_format(current_date(),"yyyyMMdd") as dt_processamento,
       |A2.qtde_registros,
       |'2' as fonte
       |from (
       |select
       |date_format(substring(p_dataset_date,1,10),"yyyyMMdd") dt_foto,
       |count(*) as qtde_registros
       |from ${database}.${table}
       |group by date_format(substring(p_dataset_date,1,10),"yyyyMMdd")
       | ) as A2
       |""".stripMargin).toDF()

  //val tabela = spark.sql(s"""select p_dataset_date , date_format(substring(p_dataset_date,1,10),"yyyyMMdd")  from ${database}.${table}""")
    log.info(s"${tabela.show(1)}")

  // System.exit(1)


  val volumetria_medidas = spark.table(s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}").as("A")
    .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
    .where(s"""concat(A.banco, A.tabela,  A.dt_processamento) <> concat('${database}','${table}', date_format(current_date(),"yyyyMMdd") )""")
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
    .orderBy("banco","tabela","dt_foto","dt_processamento")

    log.info("carregando unionAll")

  volumetria_medidas.createOrReplaceTempView("volumetria_medidas")
  val final_medidas = spark.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}
       |select  * from volumetria_medidas""".stripMargin)

    log.info(s"INSERT OVERWRITE TABLE h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}")
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
