package br.com.vivo.dataquality.duplicidade

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object ColetaDuplicidade3 extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_nome_campo: String = args(2)

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)

  val dropDF = sqlContext.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste")

  val duplicateDF = sqlContext.sql(
    s"""
  create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste
  STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
select
A2.banco,
A2.tabela,
A2.dt_foto,
A2.dt_processamento,
cast(B2.qtde1 as bigint) qtde1,
cast(C2.qtde2 as bigint) qtde2,
cast(B2.qtde1 as bigint) - cast(C2.qtde2 as bigint) diferenca

from (
   -- GRUPO 1
   select
   '$database' as banco,
   '$table' as tabela,
   date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento
) as A2

left join (
   select
   date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
   count(B1.dt_foto) qtde1
   from $database.$table as B1
) as B2
   on B2.dt_foto = A2.dt_foto
left join (
   select
   date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
   count(C1.$var_nome_campo) qtde2
   from (
      select distinct * from $database.$table
   ) as C1
) as C2
   on C2.dt_foto = A2.dt_foto
    """)


}
