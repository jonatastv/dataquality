package br.com.vivo.dataquality.volumetria


import org.apache.http.auth.AuthenticationException
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Calendar
import org.apache.log4j.{Level, Logger}
//import com.hortonworks.hwc.HiveWarehouseSession
//import com.hortonworks.hwc.HiveWarehouseSession._
//import com.hortonworks.spark.sql.hive.llap.{HiveWarehouseSession, HiveWarehouseSessionImpl}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder


object ColetaVolumetria extends App {

  val database: String = args(0)
  val table: String = args(1)
  var var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)
  val var_formato_dt_foto: String = args(4)
  val sciencedatabase : String = args(5)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)
  //log.setLevel(Level.ERROR)

  log.info(s"Iniciando o processo")
  log.info(s"Carregando parametros")
  log.info(s"database origem: $database")
  log.info(s"tabela: $table")
  log.info(s"data_foto: $var_data_foto")
  log.info(s"nome campo dt_foto: $var_nome_campo")
  log.info(s"formato data: $var_formato_dt_foto")
  log.info(s"database destino :$sciencedatabase")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)
  Logger.getLogger("com.hortonworks").setLevel(Level.OFF)
  Logger.getLogger("com.qubole").setLevel(Level.OFF)


  /**
  | Example command line to run this app:
    time spark-submit --master yarn  \
    --queue Qualidade \
    --class br.com.vivo.dataquality.volumetria.ColetaVolumetria  \
    /home/SPDQ/indra/dataquality-master_2.11.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money  20210520 dt_foto 1
   */

  try{



  val spark = SparkSession
    .builder()
    .appName(s"Volumetria_$table")
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



  val parametrosDf = spark.sql(
       s" select disponibilidade_fonte, " +
       s" disponibilidade_detalhe, " +
       s" tabela_medida, " +
       s" fim_periodo_leitura " +
         s" from $sciencedatabase.dq_parametros " +
       s" where tabela = '${table}' "
       )

  log.info(s"lendo tabela $sciencedatabase.dq_parametros")
 // log.info(s"${parametrosDf.show(1)}")

    if(parametrosDf.count() == 0){
      log.info(s"Não existe '${table}' na tabela $sciencedatabase.dq_parametros")
    }

  val disponibilidade_fonte: Array[String] = for (disponibilidade_fonte <- parametrosDf.select("disponibilidade_fonte").collect()) yield {
    disponibilidade_fonte.getString(0)
  }
  val disponibilidade_detalhe: Array[String] = for (disponibilidade_detalhe <- parametrosDf.select("disponibilidade_detalhe").collect()) yield {
    disponibilidade_detalhe.getString(0)
  }

  val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
    projeto_2.getString(0).toLowerCase
  }

  val fim_periodo_leitura: Array[String] = for (i <- parametrosDf.select("fim_periodo_leitura").collect()) yield {
    i.getString(0)
  }

    log.info(s"lendo  collect")
  for( i <-  disponibilidade_fonte.indices ) {


    if(disponibilidade_fonte(i) == "d"){

    }else if(disponibilidade_fonte(i) == "s"){

      val now = Calendar.getInstance()
      val currentHour = now.get(Calendar.DAY_OF_WEEK)
      val currentHour2 = now.get(Calendar.DAY_OF_MONTH)
      val ff = now.get(Calendar.DAY_OF_WEEK_IN_MONTH)

      val semana = disponibilidade_detalhe(i) match {
        case "segunda"  => "Mondey"
        case "terca"  => "Tuesday"
        case "quarta"  => "Wednesday"
        case "quinta" => "Thursday"
        case "sexta"  => "Friday"
        case "sabado"  => "Saturday"
        case "domingo"  => "Sanday"

        case _  => "Invalid month"  // the default, catch-all
      }

     // println(currentHour)
      //println(currentHour2)
      //println(ff)
      //2021-07-20
      //current_date()

      val dt_foto = hive.executeQuery(s"""select date_format(date_sub(next_day(current_date(),'Tuesday'),7),"yyyyMMdd") """)
        .collect().mkString("")
        .replace("[","")
        .replace("]","")

      var_data_foto = dt_foto
      //println(var_data_foto)

    }else if(disponibilidade_fonte(i) == "m"){
     // println(current_date()-1)
    }




  var validador = 0


    val partiton_df = spark.sql(s"show partitions ${database}.${table}").toDF("result")

    log.info(s"lendo partitions")
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


    validador = ff.toInt

    if (validador == 0) {
      log.info(s"não existe partição para essa dt_foto  $validador")

      val save_df = spark.sql(
        s"""
           |select '$database' as banco,
           |'$table' as tabela,
           |'$var_data_foto' as dt_foto,
           |'$var_nome_campo' as var_nome_campo,
           |'$var_formato_dt_foto' as var_formato_dt_foto,
           |0 as status
           |""".stripMargin)

      save_df.
        write.
        mode("append").
        format("orc").
        insertInto(s"${sciencedatabase}.dq_volumetria_falhas")

      log.info(s"salvo na tabela $sciencedatabase.dq_volumetria_falhas")

      val volumetria_data = hive.executeQuery(
        s"""
           |select qtde_registros
           |from $sciencedatabase.dq_volumetria_medidas${projeto(i)}
           |where tabela = '$table'
           |and banco = '$database'
           |and dt_foto = '$var_data_foto'
           |and dt_processamento = date_format(current_date(),"yyyyMMdd")""".stripMargin).count()


      var count = 0
      count = volumetria_data.toInt

      if (count == 0){

        hive.dropTable(s"$sciencedatabase.dq_volumetria_medidas_${database}_${table}_t", true, false)
//        spark.sql(s"drop table if exists $sciencedatabase.dq_volumetria_medidas_${database}_${table}_t")

        val save_medidas = hive.executeQuery(
          s"""
             |select '$database' as banco,
             |'$table' as tabela,
             |'$var_data_foto' as dt_foto,
             |date_format(current_date(),"yyyyMMdd") as dt_processamento,
             |0 as qtde_registros,
             |1 as fonte
             |""".stripMargin)


        val coleta=s"$sciencedatabase.dq_volumetria_medidas_${database}_${table}_t"
        save_medidas.write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",coleta).save()

        // hive.executeQuery(s"create Table IF NOT EXISTS $sciencedatabase.dq_volumetria_medidas_${database}_${table}_t  as select * from coleta")
        log.info(s"salvando na tabela $sciencedatabase.dq_volumetria_medidas_${database}_${table}_t")

      }

    }

  else {

      //spark.sql(s"drop table if exists $sciencedatabase.dq_volumetria_medidas_${database}_${table}_t")
      hive.dropTable(s"$sciencedatabase.dq_volumetria_medidas_${database}_${table}_t", true, false)

      log.info(s"tem partição $validador")
      log.info(s"Executando Query")

     val tabela = hive.executeQuery(
      s"""
         | -- create Table IF NOT EXISTS $sciencedatabase.dq_volumetria_medidas_${database}_${table}_t
         | -- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
         | -- STORED AS ORC TBLPROPERTIES('TRANSACTIONAL' = 'true') as
         |select
         |'${database}' as banco,
         |'${table}' as tabela,
         |'${var_data_foto}' as dt_foto,
         |date_format(current_date(),"yyyyMMdd") as dt_processamento,
         |A2.qtde_registros,
         |'1' as fonte
         |from (
         |select count(1) as qtde_registros
         |from ${database}.${table} A1
         |where
         |    case
         |         when '${var_formato_dt_foto}' = '0' then true
         |         when '${var_formato_dt_foto}' = '1' then cast(A1.${var_nome_campo} as string) = '${var_data_foto}'
         |         when '${var_formato_dt_foto}' = '2' then cast(from_unixtime(unix_timestamp(A1.${var_nome_campo},'yyyy-MM-dd'),'yyyyMMdd') as string) = '${var_data_foto}'
         |         when '${var_formato_dt_foto}' = '3' then substr(A1.${var_nome_campo},1,8) = '${var_data_foto}'
         |    end
         | ) as A2
         |""".stripMargin)


      hive.setDatabase(s"$sciencedatabase")
      hive.createTable(s"dq_volumetria_medidas_${database}_${table}_t").ifNotExists()
        .column("banco", "string")
        .column("tabela", "string")
        .column("dt_foto", "string")
        .column("dt_processamento", "string")
        .column("qtde_registros", "bigint")
        .column("fonte", "string")
        .create()


      log.info(s"salvando tabela $sciencedatabase.dq_volumetria_medidas_${database}_${table}_t")

      val tablename = s"$sciencedatabase.dq_volumetria_medidas_${database}_${table}_t"
      tabela.write.mode("append").format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector").option("table",tablename).save()

      log.info(s"Processo transformação do Hive completo")



  }

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