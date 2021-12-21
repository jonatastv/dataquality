package br.com.vivo.dataquality.volumetria

import org.apache.http.auth.AuthenticationException
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Calendar
import org.apache.log4j.{Level, Logger}


object ColetaVolumetria extends App {

  val database: String = args(0)
  val table: String = args(1)
  var var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)
  val var_formato_dt_foto: String = args(4)

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.INFO)

  log.info(s"Iniciando o processo")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)

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
       s" from h_bigd_dq_db.dq_parametros " +
       s" where tabela = '${table}' "
       )

  log.info(s"lendo tabela h_bigd_dq_db.dq_parametros")
  log.info(s"${parametrosDf.show(1)}")

    if(parametrosDf.count() == 0){
      log.info(s"Não existe '${table}' na tabela h_bigd_dq_db.dq_parametros")
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


  for( i <-  disponibilidade_fonte.indices ) {
    println("value #" + i + " is " + disponibilidade_fonte(i))
    println("value #" + i + " is " + disponibilidade_detalhe(i))

    if(disponibilidade_fonte(i) == "d"){
      println(var_data_foto)
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

      println(currentHour)
      println(currentHour2)
      println(ff)
      //2021-07-20
      //current_date()

      val dt_foto = spark.sql(s"""select date_format(date_sub(next_day(current_date(),'Tuesday'),7),"yyyyMMdd") """)
        .collect().mkString("")
        .replace("[","")
        .replace("]","")

      var_data_foto = dt_foto
      println(var_data_foto)

    }else if(disponibilidade_fonte(i) == "m"){
      println(current_date()-1)
    }




  var validador = 0


    val partiton_df = spark.sql(s"show partitions ${database}.${table}").toDF("result")

    partiton_df.registerTempTable("partitions_df")

    val ff = spark.sql(
      s"""
       select result from partitions_df
       where
       case
       when '$var_formato_dt_foto' = '1' then cast(result as string) = '$var_nome_campo=$var_data_foto'
       when '$var_formato_dt_foto' = '2' then date_format(regexp_replace(result, '$var_nome_campo=',''),"yyyyMMdd") = "$var_data_foto"
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
        insertInto("h_bigd_dq_db.dq_volumetria_falhas")

      log.info(s"salvo na tabela h_bigd_dq_db.dq_volumetria_falhas")

      val volumetria_data = spark.sql(
        s"""
           |select qtde_registros
           |from h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}
           |where tabela = '$table'
           |and banco = '$database'
           |and dt_foto = '$var_data_foto'
           |and dt_processamento = date_format(current_date(),"yyyyMMdd")""".stripMargin).count()

      var count = 0
      count = volumetria_data.toInt

      if (count == 0){

        val save_medidas = spark.sql(
          s"""
             |select '$database' as banco,
             |'$table' as tabela,
             |'$var_data_foto' as dt_foto,
             |date_format(current_date(),"yyyyMMdd") as dt_processamento,
             |0 as qtde_registros,
             |1 as fonte
             |""".stripMargin)

        save_medidas.
          write.
          mode("append").
          format("orc").
          insertInto(s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}")

      }

    }

  else {


      log.info(s"tem partição $validador")

    val tabela = spark.sql(
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
         |where
         |    case
         |       when '$var_formato_dt_foto' = '1' then cast($var_nome_campo as string) = '$var_data_foto'
         |       when '$var_formato_dt_foto' = '2' then cast(date_format($var_nome_campo,"yyyyMMdd") as string) = '$var_data_foto'
         |    end
         | ) as A2
         |""".stripMargin).toDF()

      log.info(s"${tabela.show(1)}")

//h_bigd_dq_db.dq_volumetria_medidas
   val volumetria_medidas = spark.table(s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}").as("A")
    .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
    .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}', date_format(current_date(),"yyyyMMdd") )""")
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

      log.info("carregando unionAll")

    volumetria_medidas.createOrReplaceTempView("volumetria_medidas")

  val final_medidas = spark.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_volumetria_teste
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}
       |select  * from volumetria_medidas""".stripMargin)

      log.info(s"INSERT OVERWRITE TABLE h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}")
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