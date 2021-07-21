package br.com.vivo.dataquality.volumetria

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object ColetaVolumetria4 extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)

  /**
  | Example command line to run this app:
    time spark-submit --master yarn  \
    --queue Qualidade \
    --class br.com.vivo.dataquality.volumetria.ColetaVolumetria4  \
    /home/SPDQ/indra/dataquality-master_2.11-1.1.jar p_bigd_urm tbgd_wrk_vveyes_rank_gast_site_tec_prio_erbs  20210701 dt_ref
   */


  val spark = SparkSession
    .builder()
    .appName(s"Volumetria4_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

  val tabela = spark.sql(
    s"""
       |select
       |'${database}' as banco,
       |'${table}' as tabela,
       |'${var_data_foto}' as dt_foto,
       |cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string) dt_processamento,
       |A2.qtde_registros,
       |'2' as fonte
       |from (
       |select count(*) as qtde_registros
       |from ${database}.${table} as A1
       |where A1.${var_nome_campo} = '${var_data_foto}'
       | ) as A2
       |""".stripMargin).toDF()

  tabela.show()

  val volumetria_medidas = spark.table("h_bigd_dq_db.temp_medidas_volumetria_teste").as("A")
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

  volumetria_medidas.createOrReplaceTempView("volumetria_medidas")
  val final_medidas = spark.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_volumetria_teste
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.temp_medidas_volumetria_teste
       |select distinct * from volumetria_medidas""".stripMargin)



}
