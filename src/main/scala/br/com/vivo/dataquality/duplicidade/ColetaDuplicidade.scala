package br.com.vivo.dataquality.duplicidade
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


object ColetaDuplicidade extends App {

  val database: String = args(0)
  val table: String = args(1)
  val var_data_foto: String = args(2)
  val var_nome_campo: String = args(3)
  val var_formato_dt_foto: String = args(4)

  /**
   | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money  20210520 dt_foto 1
   */

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)

 // val tabela = h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_ + ${database} + "_"+${table}+"_teste"

 // val drop = sqlContext.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste")

  val duplicateDF = sqlContext.sql(s"""

 -- create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste
 -- create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_p_bigd_urm_tbgd_turm_controle_faturas_vivo_money_teste
 -- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as

select
A2.banco,
A2.tabela,
A2.dt_foto,
A2.dt_processamento,
cast(B2.qtde1 as bigint) qtde1,
cast(C2.qtde2 as bigint) qtde2,
cast(B2.qtde1 as bigint) - cast(C2.qtde2 as bigint) diferenca,
'2' as fonte

from (
   -- GRUPO 1
   select
   '$database' as banco,
   '$table' as tabela,
   '$var_data_foto' as dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento
) as A2

left join (
   select
   '$var_data_foto' as dt_foto,
  count(B1.dt_foto) qtde1
   from $database.$table as B1

   -- filtrando a dt_foto passada
   where

    -- B1.dt_foto = '$var_data_foto'
      case
         when '$var_formato_dt_foto' = '1' then cast(B1.$var_nome_campo as string) = '$var_data_foto'
         when '$var_formato_dt_foto' = '2' then cast(date_format(B1.${var_nome_campo},"yyyyMMdd") as string) = '$var_data_foto'
      end
) as B2
   on B2.dt_foto = A2.dt_foto

left join (
   select
   '$var_data_foto' as dt_foto,
   count(C1.$var_nome_campo) qtde2
   from (
      select distinct * from $database.$table
      -- filtrando a dt_foto passada
      where
    --  dt_foto = '$var_data_foto'

         case
            when '$var_formato_dt_foto' = '1' then cast($var_nome_campo as string) = '$var_data_foto'
            when '$var_formato_dt_foto' = '2' then cast(date_format($var_nome_campo,"yyyyMMdd") as string) = '$var_data_foto'
         end

   ) as C1

) as C2
   on C2.dt_foto = A2.dt_foto
    """)

  duplicateDF.show

  //val duplicateDFnew = duplicateDF.withColumn("fonte",lit(2))

  //duplicateDF.show

/*
  val medidasDF  = sqlContext.table("h_bigd_dq_db.dq_duplicados_medidas").alias("A")
    .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde1","A.qtde2","A.diferenca")
    .withColumn("fonte",lit(1))
   // .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}', date_format(current_date(),"yyyyMMdd") )""")
    .where(col("banco") !== database)
    .where(col("tabela") !== table)
    .where(col("dt_foto") !== var_data_foto)
    .where(col("dt_processamento") !== date_format(current_date(),"yyyyMMdd"))
    .unionAll(
        duplicateDF.select(
          duplicateDF.col("banco"),
          duplicateDF.col("tabela"),
          duplicateDF.col("dt_foto"),
          duplicateDF.col("dt_processamento"),
          duplicateDF.col("qtde1"),
          duplicateDF.col("qtde2"),
          duplicateDF.col("diferenca"),
          duplicateDF.col("fonte")
          //duplicateDF.withColumn("fonte",lit(2))
        )
    )
    .toDF()
*/


  //h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}
//h_bigd_dq_db.${var_tabela_auxiliar}


    sqlContext.setConf("hive.exec.dynamic.partition.mode" ,"nonstrict")
    sqlContext.setConf("hive.exec.dynamic.partition" ,"true")



  duplicateDF.
      write.
      mode("append").
      format("orc").
      insertInto("h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_p_bigd_urm_tbgd_turm_controle_faturas_vivo_money_teste")
      //h_bigd_dq_db.dq_duplicados_medidas

}
