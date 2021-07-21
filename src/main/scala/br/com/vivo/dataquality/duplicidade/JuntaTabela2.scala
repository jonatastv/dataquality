package br.com.vivo.dataquality.duplicidade


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.{File, PrintWriter, StringWriter}

object JuntaTabela2 extends  App {

  val database: String = args(0)
  val table: String = args(1)
  val table_auxiliar: String = args(2)

  /**
  | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.duplicidade.JuntaTabela \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money 20210520 dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t

   */

  val spark = SparkSession
    .builder()
    .appName(s"juntaTabela_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

  val erroutput = new StringWriter

  val tabela = spark.sql(
    s"""
       |select
       |--'${database}'  x1,
       |--'${table}' x2,
       |--B.dt_foto x3,
       |--B.dt_processamento x4,
       |---
       |A.banco,
       |A.tabela,
       |A.dt_foto,
       |A.dt_processamento,
       |A.qtde1,
       |A.qtde2,
       |A.diferenca,
       |'1' as fonte
       |
       |from h_bigd_dq_db.dq_duplicados_medidas_aux_junta_tabela_teste A
       |left join h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t B
       |   on B.banco = A.banco
       |   and B.tabela = B.tabela
       |where
       |-- FILTRO DO BANCO + TABELA + DT_PROCESSAMENTO
       |concat (
       |   trim(nvl(A.banco,'')), '|',
       |   trim(nvl(A.tabela,'')), '|',
       |   trim(nvl(A.dt_foto,'')), '|',
       |   trim(nvl(A.dt_processamento,''))
       |)
       |   <>
       |concat (
       |   trim(nvl('${database}','')), '|',
       |   trim(nvl('${table}','')), '|',
       |   trim(nvl(B.dt_foto,'')), '|',
       |   trim(nvl(B.dt_processamento,''))
       |
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
       |
       |order by dt_foto
       |""".stripMargin)


  tabela.registerTempTable("temp")

  val cretedf = spark.sql(
    s"""
       |insert overwrite table h_bigd_dq_db.dq_duplicados_medidas_aux_junta_tabela_teste
       |select distinct * from temp
       |""".stripMargin)

}
