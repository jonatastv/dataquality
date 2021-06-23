package br.com.vivo.dataquality.volumetria

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object ColetaVolumetria3 extends App {

  val database: String = args(0)
  val table: String = args(1)

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)

  val tabela = sqlContext.sql(
    s"""
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

    tabela.show

  val volumetria_medidas = sqlContext.table("h_bigd_dq_db.temp_medidas_volumetria_teste").as("A")
    .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
    //.where(s"""concat(A.banco, A.tabela, A.dt_foto, A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}',cast(date_format(current_date(),"yyyyMMdd") as string)""")
    .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}',date_format(date_sub(current_date() , 1 ),"yyyyMMdd"), date_format(current_date(),"yyyyMMdd") )""")
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


  volumetria_medidas.registerTempTable("volumetria_medidas")
  val final_medidas = sqlContext.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_volumetria_teste
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.temp_medidas_volumetria_teste
       |select  * from volumetria_medidas""".stripMargin)

}
