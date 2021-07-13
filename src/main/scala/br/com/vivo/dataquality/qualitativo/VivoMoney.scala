package br.com.vivo.dataquality.qualitativo

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, LongType,StructType, StructField}

object VivoMoney extends App {

  val var_dt_foto: String = args(0)

  /**
  | Example command line to run this app:
    time spark-submit --master yarn  \
   --queue Qualidade \
   --class br.com.vivo.dataquality.qualitativo.vivoMoney  \
    /home/SPDQ/indra/dataquality_2.10-0.1.jar 20210629
   */

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)

  val sqlVivoMoney = sqlContext.sql(
    s"""
       |select
       |A.dt_foto,
       |A.dt_processamento,
       |A.tipo_linha_des,
       |A.status_des,
       |A.regional_des,
       |A.tempo_relacionamento_nr,
       |A.qtde,
       |A.cpfs_unicos,
       |A.terminais_unicos,
       |A.plano_vl,
       |A.fatura_vl,
       |A.qtd_produto_nr,
       |A.qtd_sva_nr,
       |'1' as fonte
       |from h_bigd_dq_db.dq_qualitativo2A_medidas as A
       |where
       |-- FILTRO DO BANCO + TABELA + DT_FOTO + DT_PROCESSAMENTO
       |concat (
       |   A.dt_foto,
       |   A.dt_processamento
       |)
       |   <>
       |concat (
       |   '${var_dt_foto}',
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
       |---
       |B2.tipo_linha_des,
       |B2.status_des,
       |B2.regional_des,
       |B2.tempo_relacionamento_nr,
       |---
       |B2.qtde,
       |B2.cpfs_unicos,
       |B2.terminais_unicos,
       |---
       |B2.plano_vl,
       |B2.fatura_vl,
       |B2.qtd_produto_nr,
       |B2.qtd_sva_nr,
       |'2' as fonte
       |
       |from (
       |   select
       |   '${var_dt_foto}' as dt_foto,
       |   date_format(current_date(),"yyyyMMdd") dt_processamento
       |) as  A
       |
       |left join (
       |   select
       |   '${var_dt_foto}' as dt_foto,
       |   date_format(current_date(),"yyyyMMdd") dt_processamento,
       |   ---
       |   B1.tipo_linha_des,
       |   B1.status_des,
       |   B1.regional_des,
       |   case
       |      when B1.tempo_relacionamento_nr <= 20 then '0-20'
       |      when B1.tempo_relacionamento_nr <= 30 then '21-30'
       |      when B1.tempo_relacionamento_nr <= 50 then '31-50'
       |      when B1.tempo_relacionamento_nr <= 60 then '51-60'
       |      else '61-99'
       |   end as tempo_relacionamento_nr,
       |   ---
       |   count(*) qtde,
       |   count(distinct B1.cpf_nr) cpfs_unicos,
       |   count(distinct B1.celular_nr) terminais_unicos,
       |   ---
       |   cast(sum(B1.plano_vl       ) as decimal(30,2)) plano_vl,
       |   cast(sum(B1.fatura_vl      ) as decimal(30,2)) fatura_vl,
       |   cast(sum(B1.qtd_produto_nr ) as decimal(30,2)) qtd_produto_nr,
       |   cast(sum(B1.qtd_sva_nr     ) as decimal(30,2)) qtd_sva_nr
       |
       |   from p_bigd_urm.tbgd_turm_vivo_money as B1
       |
       |   where B1.dt_foto = '${var_dt_foto}'
       |
       |   group by
       |   '${var_dt_foto}',
       |   date_format(current_date(),"yyyyMMdd"),
       |   ---
       |   B1.tipo_linha_des,
       |   B1.status_des,
       |   B1.regional_des,
       |   case
       |      when B1.tempo_relacionamento_nr <= 20 then '0-20'
       |      when B1.tempo_relacionamento_nr <= 30 then '21-30'
       |      when B1.tempo_relacionamento_nr <= 50 then '31-50'
       |      when B1.tempo_relacionamento_nr <= 60 then '51-60'
       |      else '61-99'
       |   end
       |) as B2
       |   on  B2.dt_foto          = A.dt_foto
       |   and B2.dt_processamento = A.dt_processamento
       |order by dt_foto
       |""".stripMargin).show()

}
