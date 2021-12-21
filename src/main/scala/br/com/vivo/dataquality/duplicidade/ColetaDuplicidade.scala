package br.com.vivo.dataquality.duplicidade
import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import scala.util.{Failure, Success, Try}
import java.io.{File, PrintWriter, StringWriter}
import org.apache.spark.sql.types._


object ColetaDuplicidade extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)
  val var_formato_dt_foto: String = args(4)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)
  Logger.getLogger("com.hortonworks").setLevel(Level.OFF)
  Logger.getLogger("com.qubole").setLevel(Level.OFF)



    try{


  /**
   | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money  20210520 dt_foto 1

 CREATE TABLE IF NOT EXISTS h_bigd_dq_db.temp_dtfoto_teste ( dt_foto String, valor String)
STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');

   */

  val spark = SparkSession
    .builder()
    .appName(s"Duplicidade_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.port.maxRetries", "100")
    .enableHiveSupport()
    .getOrCreate()

  log.info(s"Iniciando aplicação spark")

  val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()
  spark.conf.set("hive.tez.queue.name","Qualidade")
  spark.conf.set("mapreduce.map.memory","5120")
  spark.conf.set("mapreduce.reduce.memory","5120")

//  hive.executeUpdate("set hive.tez.queue.name=Qualidade")

  val applicationId: String = spark.sparkContext.applicationId

  log.info(s"**********************************************************************************")
  log.info(s"*** Application ID: $applicationId")
  log.info(s"**********************************************************************************")


 // val tabela = h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_ + ${database} + "_"+${table}+"_teste"
 // try {

  log.info(s"Verificando se existe partição")
 val partiton_df = spark.sql(s"show partitions ${database}.${table}").toDF("result")

  //partiton_df.orderBy(desc("result")).show()

  partiton_df.registerTempTable("partitions_df")

 val ff = spark.sql(
   s"""
       select result from partitions_df
       where
       case
       when '$var_formato_dt_foto' = '1' then cast(result as string) = '$var_nome_campo=$var_data_foto'
       when '$var_formato_dt_foto' = '2' then date_format(regexp_replace(result, '$var_nome_campo=',''),"yyyy-MM-dd") = "$var_data_foto"
       end
       """).count()

  //println(ff)

  if (ff == 0) {

    log.info(s"não existe partição para a dt_foto = $var_data_foto")
    val save_df = hive.executeQuery(
      s"""
         |select '$database' as banco,
         |'$table' as tabela,
         |'$var_data_foto' as dt_foto,
         |'${var_nome_campo}' as var_nome_campo,
         |'${var_formato_dt_foto}' as var_formato_dt_foto,
         |'0' as status
         |""".stripMargin)

    val falhas = "h_bigd_dq_db.dq_duplicidade_falhas2"

    save_df.write.mode("append").format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",falhas).save()

//      save_df.
//      write.
//      mode("append").
//      format("orc").
//      insertInto("h_bigd_dq_db.dq_duplicidade_falhas2")

    log.info(s"salvando na tabela h_bigd_dq_db.dq_duplicidade_falhas2")

    val parametrosDf = spark.sql(
      s"""
         |select disponibilidade_fonte ,
         |disponibilidade_detalhe ,
         |tabela_medida,
         |tabela
         |from h_bigd_dq_db.dq_parametros
         |where tabela = '${table}'
         |""".stripMargin)

    log.info(s"lendo tabela h_bigd_dq_db.dq_parametros")
   // log.info(s"${parametrosDf.show(1)}")

    if(parametrosDf.count() == 0){
      log.info(s"Não existe '${table}' na tabela h_bigd_dq_db.dq_parametros")
    }


    val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
      projeto_2.getString(0).toLowerCase
    }
    var projetos = ""
    for( i <-  projeto.indices ) {
      projetos = projeto(i)
    }


    val duplicidade_data = hive.executeQuery(
      s"""
         |select qtde1
         |from h_bigd_dq_db.dq_duplicados_medidas${projetos}
         |where tabela = '$table'
         |and banco = '$database'
         |and dt_foto = '$var_data_foto'
         |and dt_processamento = date_format(current_date(),"yyyyMMdd")""".stripMargin).count()

    //println(duplicidade_data)
    var count = 0
    count = duplicidade_data.toInt

    if (count == 0){

      hive.dropTable(s"h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}", true, false)

      val save_duplicidade = hive.executeQuery(
        s"""
           |select '$database' as banco,
           |'$table' as tabela,
           |'$var_data_foto' as dt_foto,
           |date_format(current_date(),"yyyyMMdd") as dt_processamento,
           |0 as qtde1,
           |0 as qtde2,
           |0 as diferenca,
           |1 as fonte
           |""".stripMargin)


      val coleta = s"h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}"
      save_duplicidade.write.mode("append").format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",coleta).save()
      log.info(s"salvando na tabela h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}")
    //  log.info(s"${save_duplicidade.show(1)}")

    }

            }
  else {
    log.info(s"partição encontrada ")
    log.info(s"realizando drop table if exists h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto} ")

    hive.dropTable(s"h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}", true, false)

    log.info(s"executando query")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    val duplicateDF = hive.executeQuery(
      s"""

 -- create Table IF NOT EXISTS h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}
 -- create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_p_bigd_urm_tbgd_turm_controle_faturas_vivo_money_teste
 -- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as

select
A.banco,
A.tabela,
A.dt_foto,
A.dt_processamento,
cast('' as bigint) qtde1,
cast((B2.qtde2) as bigint) qtde2,
cast('' as bigint) diferenca

from (
   -- GRUPO 1
   select
   '$database' as banco,
   '$table' as tabela,
   '$var_data_foto' as dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento
) as A

left join (
   -- GRUPO 2
   select
   '${var_data_foto}' as dt_foto,
   count(B1.${var_nome_campo}) qtde2
   from (
      select distinct * from ${database}.${table}
      -- filtrando a dt_foto passada
      where
         case
            when '${var_formato_dt_foto}' = '0' then true
            when '${var_formato_dt_foto}' = '1' then cast(${var_nome_campo} as string) = '${var_data_foto}'
            when '${var_formato_dt_foto}' = '2' then cast(from_unixtime(unix_timestamp(${var_nome_campo},'yyyy-MM-dd'),'yyyyMMdd') as string) = '${var_data_foto}'
            when '${var_formato_dt_foto}' = '3' then substr(${var_nome_campo},1,8) = '${var_data_foto}'
         end
   ) B1

) B2
   on B2.dt_foto = A.dt_foto
    """)

    log.info(s"salvando na tabela h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}")

    hive.setDatabase(s"h_bigd_dq_db")
    hive.createTable(s"dq_d_${database}_${table}_${var_data_foto}").ifNotExists()
      .column("banco", "string")
      .column("tabela", "string")
      .column("dt_foto", "string")
      .column("dt_processamento", "string")
      .column("qtde1", "bigint")
      .column("qtde2", "bigint")
      .column("diferenca", "bigint")
      .create()

    val tablename = s"h_bigd_dq_db.dq_d_${database}_${table}_${var_data_foto}"
    duplicateDF.write.mode("append").format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",tablename).save()

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

/*
  } catch
  {
    case _: Throwable => val temperror = spark.sql(
      s"""
         | -- insert overwrite table h_bigd_dq_db.temp_dtfoto_teste
         |select '$database' as banco,
         |'$table' as tabela,
         |'$var_data_foto' as dt_foto,
         |'${var_nome_campo}' as var_nome_campo,
         |'${var_formato_dt_foto}' as var_formato_dt_foto,
         |'0' as status
         |""".stripMargin)

      temperror.
        distinct().
        write.
        mode("append").
        format("orc").
        insertInto("h_bigd_dq_db.dq_duplicidade_falhas")
*/
      /*val ff = spark.sql(
      s"""
         create Table IF NOT EXISTS h_bigd_dq_db.temp_dtfoto_teste

          STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
          select * from teste

         """)
      **/
    /*d.printStackTrace(new PrintWriter(erroutput))
      new PrintWriter(s"/tmp/yspark_log") //Saves error message to this location
      {
        write(erroutput.toString);

        close
      }

  }

     */

}
