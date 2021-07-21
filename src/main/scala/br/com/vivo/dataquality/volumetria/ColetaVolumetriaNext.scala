package br.com.vivo.dataquality.volumetria

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveContext

object ColetaVolumetriaNext extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)
  val var_formato_dt_foto: String = args(4)


  /**
  | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.volumetria.ColetaVolumetria  \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money  20210520 dt_foto 1
   */

  val spark = SparkSession
    .builder()
    .appName(s"Volumetria_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()




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
         |where p_state = 'published'
         |and   p_workflow_id = 'event-compacted'
         |and   p_attempt = '0'
         |and
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

    volumetria_medidas.registerTempTable("volumetria_medidas")
  val final_medidas = spark.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_volumetria_teste
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.temp_medidas_volumetria_teste
       |select  * from volumetria_medidas""".stripMargin)

    //  .where(col("A.tabela") === "${table}")
    //.limit(5)



}