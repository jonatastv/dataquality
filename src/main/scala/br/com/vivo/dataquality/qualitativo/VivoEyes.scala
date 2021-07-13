package br.com.vivo.dataquality.qualitativo

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, LongType,StructType, StructField}

object VivoEyes extends App {

  /**
  | Example command line to run this app:
    time spark-submit --master yarn  \
   --queue Qualidade \
   --class br.com.vivo.dataquality.qualitativo.vivoEyes  \
    /home/SPDQ/indra/dataquality_2.10-0.1.jar
   */

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)



  val sqlVivoEyes = sqlContext.sql(
    s"""
       |create Table h_bigd_dq_db.dq_qualitativo3_medidas_t
       |STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |select
       |A.dt_foto,
       |A.dt_processamento,
       |A.cluster,
       |A.erb,
       |A.regional,
       |A.tecnologia,
       |A.faixa_score,
       |A.controladora,
       |A.contratada,
       |A.receita,
       |A.trfg_minutos,
       |A.trfg_mb,
       |'1' as fonte
       |
       |from h_bigd_dq_db.dq_qualitativo3_medidas_t as A
       |where
       |-- FILTRO DT_FOTO + DT_PROCESSAMENTO
       |concat (
       |   A.dt_foto,
       |   A.dt_processamento
       |)
       |   <>
       |concat (
       |   date_format(current_date(),"yyyyMM"),
       |   date_format(current_date(),"yyyyMMdd")
       |)
       |
       |----------
       |union all
       |----------
       |
       |select
       |A.dt_foto,
       |A.dt_processamento,
       |----------
       |B2.cluster,
       |B2.erb,
       |B2.regional,
       |B2.tecnologia,
       |B2.faixa_score,
       |B2.controladora,
       |B2.contratada,
       |----------
       |B2.receita,
       |B2.trfg_minutos,
       |B2.trfg_mb,
       |
       |'2' as fonte
       |
       |from (
       |   select
       |   date_format(current_date(),"yyyyMM") as dt_foto,
       |   date_format(current_date(),"yyyyMMdd") as dt_processamento
       |) as A
       |left join (
       |   select
       |   date_format(current_date(),"yyyyMM") as dt_foto,
       |   date_format(current_date(),"yyyyMMdd") as dt_processamento,
       |   B1.cluster,
       |   B1.erb,
       |   B1.regional,
       |   B1.tecnologia,
       |   B1.faixa_score,
       |   B1.controladora,
       |   B1.contratada,
       |
       |   cast(sum(B1.receita) as decimal(30,2)) receita,
       |   cast(sum(B1.trfg_minutos) as decimal(30,2)) trfg_minutos,
       |   cast(sum(B1.trfg_mb) as decimal(30,2)) trfg_mb
       |
       |   from h_bigd_vivoeyes_db.tbgd_wrk_vveyes_rank_gast_site_tec_prio_erbs as B1
       |
       |   group by
       |   date_format(current_date(),"yyyyMM"),
       |   date_format(current_date(),"yyyyMMdd"),
       |   B1.cluster,
       |   B1.erb,
       |   B1.regional,
       |   B1.tecnologia,
       |   B1.faixa_score,
       |   B1.controladora,
       |   B1.contratada
       |) as B2
       |   on  B2.dt_foto          = A.dt_foto
       |   and B2.dt_processamento = A.dt_processamento
       |   order by dt_foto
       |""".stripMargin)


  sqlVivoEyes.registerTempTable("sqlVivoEyes")
 /*
  val final_medidas = sqlContext.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_volumetria_teste
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.temp_medidas_volumetria_teste
       |select  * from volumetria_medidas""".stripMargin)
*/


}
