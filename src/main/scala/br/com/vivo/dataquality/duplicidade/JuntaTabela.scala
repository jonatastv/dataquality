package br.com.vivo.dataquality.duplicidade

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

import java.io.{File, PrintWriter, StringWriter}

object JuntaTabela extends  App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val var_tabela_auxiliar: String = args(3)

/**
| Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.duplicidade.JuntaTabela \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money 20210520 dq_duplicados_medidas_aux_01_coletaDuplicidade_p_bigd_urm_tbgd_turm_controle_faturas_vivo_money_teste

*/

  //val sc = new SparkContext(new SparkConf() )
  val spark = SparkSession
    .builder()
    .appName(s"juntaTabela_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.port.maxRetries", "100")
    .enableHiveSupport()
    .getOrCreate()

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

    // val dropTabelaAux = spark.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}")
    val tabela = spark.sql(
      s"""
         |-- create Table h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}
         |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
         |
         |-- HISTORICO DO CUBO
         |select
         |A.banco,
         |A.tabela,
         |A.dt_foto,
         |A.dt_processamento,
         |A.qtde1,
         |A.qtde2,
         |A.diferenca,
         |'1' as fonte
         |
         |from h_bigd_dq_db.dq_duplicados_medidas${projeto(i)} A
         |where
         |-- FILTRO DO BANCO + TABELA + DT_FOTO + DT_PROCESSAMENTO
         |concat (
         |   A.banco,
         |   A.tabela,
         |   A.dt_foto,
         |   A.dt_processamento
         |)
         |   <>
         |concat (
         |   '${database}',
         |   '${table}',
         |   '${var_data_foto}',
         |   date_format(current_date(),"yyyyMMdd")
         |)
         |
         |----------
         |union all
         |----------
         |
         |select
         |A.banco,
         |A.tabela,
         |A.dt_foto,
         |A.dt_processamento,
         |A.qtde1,
         |A.qtde2,
         |A.diferenca,
         |'2' as fonte
         |from h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t A
         |order by dt_foto
         |""".stripMargin).toDF()

    tabela.createOrReplaceTempView("temp")

    val cretedf = spark.sql(
      s"""
         |insert overwrite table h_bigd_dq_db.dq_duplicados_medidas${projeto(i)}
         |select distinct * from temp
         |""".stripMargin)

  }
}
