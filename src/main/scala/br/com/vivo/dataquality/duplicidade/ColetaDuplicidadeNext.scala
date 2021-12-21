package br.com.vivo.dataquality.duplicidade

import org.apache.http.auth.AuthenticationException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

import java.io.{File, PrintWriter, StringWriter}

object ColetaDuplicidadeNext extends  App {

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

  try{

  /**
  | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.duplicidade.ColetaDuplicidadeNext \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar h_bigd_ods_db com_amdocs_insight_dh_shared_mutables_payment_paymentmutable  20210520 p_date 2

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
  val erroutput = new StringWriter

    log.info(s"Iniciando aplicação spark")

    val applicationId: String = spark.sparkContext.applicationId

    log.info(s"**********************************************************************************")
    log.info(s"*** Application ID: $applicationId")
    log.info(s"**********************************************************************************")


  //try {

    log.info(s"Verificando se existe partição")
    val partiton_df = spark.sql(s"show partitions ${database}.${table}").toDF("result")

    //partiton_df.orderBy(desc("result")).show()

    partiton_df.createOrReplaceTempView("partitions_df")

    val ff = spark.sql(
      s"""
       select result from partitions_df
       where
       case
       when '$var_formato_dt_foto' = '1' then cast(result as string) = '$var_nome_campo=$var_data_foto'
       when '$var_formato_dt_foto' = '2' then date_format(substr(result, ${var_nome_campo.size} +2  ,10), "yyyyMMdd") = "$var_data_foto"
       end
       """).count()



    if (ff == 0) {
      log.info(s"não existe partição para a dt_foto = $var_data_foto")

      val save_df = spark.sql(
        s"""
           |select '$database' as banco,
           |'$table' as tabela,
           |'$var_data_foto' as dt_foto,
           |'${var_nome_campo}' as var_nome_campo,
           |'${var_formato_dt_foto}' as var_formato_dt_foto,
           |'0' as status
           |""".stripMargin)

      save_df.
        write.
        mode("append").
        format("orc").
        insertInto("h_bigd_dq_db.dq_duplicidade_falhas")

      log.info(s"salvando na tabela h_bigd_dq_db.dq_duplicidade_falhas")


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
      var projetos = ""
      for( i <-  projeto.indices ) {
        projetos = projeto(i)
      }


      val duplicidade_data = spark.sql(
        s"""
           |select qtde1
           |from h_bigd_dq_db.dq_duplicados_medidas${projetos}
           |where tabela = '$table'
           |and banco = '$database'
           |and dt_foto = '$var_data_foto'
           |and dt_processamento = date_format(current_date(),"yyyyMMdd")""".stripMargin).count()

      println(duplicidade_data)
      var count = 0
      count = duplicidade_data.toInt

      if (count == 0) {

        spark.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t")

        val save_duplicidade = spark.sql(
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

        save_duplicidade.createOrReplaceTempView("coleta")

        spark.sql(s"create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t  as select * from coleta")

        log.info(s"salvando na tabela h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t")
        log.info(s"${save_duplicidade.show(1)}")
      }

    }
    else {
      log.info(s"partição encontrada ")
      log.info(s"realizando drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t ")

      val dropDF = spark.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t")

      log.info(s"executando query")
      val duplicateDF = spark.sql(
        s"""

  create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t
  STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as

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
   '$database' as banco,
   '$table' as tabela,
   '$var_data_foto' as dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento
) as A2

left join (
   select
   '$var_data_foto' as dt_foto,
  count(B1.${var_nome_campo}) qtde1
   from $database.$table as B1

   -- filtrando a dt_foto passada
   where B1.p_state = 'published'
   and B1.p_workflow_id = 'event-compacted'
   and B1.p_attempt = '0'
   and
      case
         when '$var_formato_dt_foto' = '1' then cast(B1.$var_nome_campo as string) = '$var_data_foto'
         when '$var_formato_dt_foto' = '2' then cast(date_format(B1.${var_nome_campo},"yyyyMMdd") as string) = '$var_data_foto'
      end
) as B2
   on B2.dt_foto = A2.dt_foto

left join (
   select
   '$var_data_foto' as dt_foto,
   count(C1.$var_nome_campo) qtde2
   from (
      select distinct * from $database.$table
      -- filtrando a dt_foto passada
      where p_state = 'published'
          and p_workflow_id = 'event-compacted'
          and p_attempt = '0'
          and
         case
            when '$var_formato_dt_foto' = '1' then cast($var_nome_campo as string) = '$var_data_foto'
            when '$var_formato_dt_foto' = '2' then cast(date_format($var_nome_campo,"yyyyMMdd") as string) = '$var_data_foto'
         end

   ) as C1

) as C2
   on C2.dt_foto = A2.dt_foto
    """)

      log.info(s"salvando na tabela h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t")
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

 /* } catch
  {
    case _: Throwable => val temperror = spark.sql(
      s"""
         |select distinct  '$database' as banco,
         |'$table' as tabela,
         |'$var_data_foto' as dt_foto,
         | date_format(current_date(),"yyyyMMdd") as dt_processamento,
         |'0' as status
         |""".stripMargin)

      temperror.
        distinct().
        write.
        mode("append").
        format("orc").
        insertInto("h_bigd_dq_db.temp_dtfoto_teste")


  }
*/


}
