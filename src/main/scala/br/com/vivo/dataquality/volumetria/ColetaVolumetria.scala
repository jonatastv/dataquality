package br.com.vivo.dataquality.volumetria

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Calendar


object ColetaVolumetria extends App {

  val database: String = args(0)
  val table: String = args(1)
  var var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)
  val var_formato_dt_foto: String = args(4)


  /**
  | Example command line to run this app:
    time spark-submit --master yarn  \
    --queue Qualidade \
    --class br.com.vivo.dataquality.volumetria.ColetaVolumetria  \
    /home/SPDQ/indra/dataquality-master_2.11.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money  20210520 dt_foto 1
   */


  val spark = SparkSession
    .builder()
    .appName(s"Volumetria_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

  val parametrosDf = spark.sql(
    s"""
       |select disponibilidade_fonte ,
       |disponibilidade_detalhe
       |from h_bigd_dq_db.dq_parametros
       |where tabela = '${table}'
       |""".stripMargin)



  val disponibilidade_fonte: Array[String] = for (disponibilidade_fonte <- parametrosDf.select("disponibilidade_fonte").collect()) yield {
    disponibilidade_fonte.getString(0)
  }
  val disponibilidade_detalhe: Array[String] = for (disponibilidade_detalhe <- parametrosDf.select("disponibilidade_detalhe").collect()) yield {
    disponibilidade_detalhe.getString(0)
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

      val dt_foto = spark.sql(s"""select date_format(date_sub(next_day(current_date(),'${semana}'),7),"yyyyMMdd") """)
        .collect().mkString("")
        .replace("[","")
        .replace("]","")

      var_data_foto = dt_foto
      println(var_data_foto)

    }else if(disponibilidade_fonte(i) == "m"){
      println(current_date()-1)
    }

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

    println(ff)

    validador = ff.toInt

    if (validador == 0) {
      println("não existe partição para essa dt_foto " + validador)

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

    }

  else {


    println("tem partição " +validador )

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

//h_bigd_dq_db.dq_volumetria_medidas
   val volumetria_medidas = spark.table("h_bigd_dq_db.temp_medidas_volumetria_teste").as("A")
    .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
    //.where(s"""concat(A.banco, A.tabela, A.dt_foto, A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}',cast(date_format(current_date(),"yyyyMMdd") as string)""")
    .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}', date_format(current_date(),"yyyyMMdd") )""")
    // .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}', date_format(current_date(),"yyyyMMdd") )""")
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
    //.show(200,false)

    volumetria_medidas.createOrReplaceTempView("volumetria_medidas")
  val final_medidas = spark.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_volumetria_teste
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.temp_medidas_volumetria_teste
       |select  * from volumetria_medidas""".stripMargin)

    //  .where(col("A.tabela") === "${table}")
    //.limit(5)


  }

}