package br.com.vivo.dataquality.flop

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object CorrigirBuracoDuplicidade extends App {


  /**
  | Example command line to run this app:
   | time spark-submit \
  --master yarn \
  --executor-memory 4g \
  --executor-cores 10 \
  --num-executors 100 \
  --queue Qualidade \
  --class br.com.vivo.dataquality.flop.CorrigirBuracoDuplicidade \
  /home/SPDQ/indra/dataquality_2.10-0.1.jar

   */

/**
    """
    | CREATE TABLE h_bigd_dq_db.dq_duplicidade_falhas(
    |   banco string,
    |   tabela string,
    |   dt_foto string,
    |   var_nome_campo string,
    |   var_formato_dt_foto string,
    |   status bigint
    |   ) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY')
    |   """.stripMargin
*/

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)
  //sc.setLogLevel("ERROR")


  val dfsql = sqlContext.sql(
    s"""
       select distinct *
       from h_bigd_dq_db.dq_duplicidade_falhas
       where status = 0
       and tabela not in ('tbgd_turm_customer')
       """).toDF()

  dfsql.show()

  val database: Array[String] = for (database <- dfsql.select("banco").collect()) yield {
    database.getString(0)

  }

  val table: Array[String] = for (table <- dfsql.select("tabela").collect()) yield {
    table.getString(0)

  }

  val var_data_foto: Array[String] = for (var_data_foto <- dfsql.select("dt_foto").collect()) yield {
    var_data_foto.getString(0)

  }

  val var_nome_campo: Array[String] = for (var_nome_campo <- dfsql.select("var_nome_campo").collect()) yield {
    var_nome_campo.getString(0)

  }

  val var_formato_dt_foto: Array[String] = for (var_formato_dt_foto <- dfsql.select("var_formato_dt_foto").collect()) yield {
    var_formato_dt_foto.getString(0)

  }

  for( i <-  database.indices ){
    println("value #" + i + " is " + database(i))
    println("value #" + i + " is " + table(i))

    val partiton_df = sqlContext.sql(s"show partitions ${database(i)}.${table(i)}").toDF("result")

    partiton_df.orderBy(desc("result")).show()

    partiton_df.registerTempTable("partitions_df")

    val ff = sqlContext.sql(
      s"""
       select result from partitions_df
       where
       case
       when '${var_formato_dt_foto(i)}' = '1' then cast(result as string) = '${var_nome_campo(i)}=${var_data_foto(i)}'
       when '${var_formato_dt_foto(i)}' = '2' then date_format(regexp_replace(result, '${var_nome_campo(i)}=',''),"yyyyMMdd") = "${var_data_foto(i)}"
       end
       """).count()

    println(ff)


    if (ff == 0) {
      println("não existe partição para essa dt_foto "+ff)


    }
    else {

      val dropDF = sqlContext.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database(i)}_${table(i)}_teste")

      val duplicateDF = sqlContext.sql(
        s"""

  create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database(i)}_${table(i)}_teste
 -- create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_p_bigd_urm_tbgd_turm_controle_faturas_vivo_money_teste
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
   '${database(i)}' as banco,
   '${table(i)}' as tabela,
   '${var_data_foto(i)}' as dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento
) as A2

left join (
   select
   '${var_data_foto(i)}' as dt_foto,
  count(B1.dt_foto) qtde1
   from ${database(i)}.${table(i)} as B1

   -- filtrando a dt_foto passada
   where

    -- B1.dt_foto = '${var_data_foto(i)}'
      case
         when '${var_formato_dt_foto(i)}' = '1' then cast(B1.${var_nome_campo(i)} as string) = '${var_data_foto(i)}'
         when '${var_formato_dt_foto(i)}' = '2' then cast(date_format(B1.${var_nome_campo(i)},"yyyyMMdd") as string) = '${var_data_foto(i)}'
      end
) as B2
   on B2.dt_foto = A2.dt_foto

left join (
   select
   '${var_data_foto(i)}' as dt_foto,
   count(C1.${var_nome_campo(i)}) qtde2
   from (
      select distinct * from ${database(i)}.${table(i)}
      -- filtrando a dt_foto passada
      where
    --  dt_foto = '${var_data_foto(i)}'

         case
            when '${var_formato_dt_foto(i)}' = '1' then cast(${var_nome_campo(i)} as string) = '${var_data_foto(i)}'
            when '${var_formato_dt_foto(i)}' = '2' then cast(date_format(${var_nome_campo(i)},"yyyyMMdd") as string) = '${var_data_foto(i)}'
         end

   ) as C1

) as C2
   on C2.dt_foto = A2.dt_foto
    """)


     // val var_tabela_auxiliar = s"h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database(i)}_${table(i)}_teste"

      val tabela = sqlContext.sql(
        s"""
           |-- create Table h_bigd_dq_db.dq_duplicados_medidas_aux_junta_tabela_teste
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
           |from h_bigd_dq_db.dq_duplicados_medidas_aux_junta_tabela_teste A
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
           |   '${database(i)}',
           |   '${table(i)}',
           |   '${var_data_foto(i)}',
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
           |from h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database(i)}_${table(i)}_teste A
           |order by dt_foto
           |""".stripMargin).toDF()

      tabela.registerTempTable("temp")

      val cretedf = sqlContext.sql(
        s"""
           |insert overwrite table h_bigd_dq_db.dq_duplicados_medidas_aux_junta_tabela_teste
           |select distinct * from temp
           |""".stripMargin)


      println("passou aqui!")


      val duplicidade_corrigido = sqlContext.sql(
        s"""
           |select
           |'${database(i)}' as banco,
           |'${table(i)}' as tabela,
           |'${var_data_foto(i)}' as dt_foto,
           |'${var_nome_campo(i)}' as var_nome_campo,
           |'${var_formato_dt_foto(i)}' as var_formato_dt_foto,
           |1 as status
           |
           |""".stripMargin)

      val duplicidade = sqlContext.sql(
        s"""
           |select distinct *
           |from h_bigd_dq_db.dq_duplicidade_falhas
           |where
           |tabela not in ('tbgd_turm_customer')
           |""".stripMargin)

      val dq_duplicidade_falhas = duplicidade.select("banco","tabela","dt_foto","var_nome_campo","var_formato_dt_foto","status")
        .where(s"""concat(banco, tabela, dt_foto) <> concat('${database(i)}','${table(i)}','${var_data_foto(i)}')""")
        .unionAll(
          duplicidade_corrigido.select(
            duplicidade_corrigido.col("banco"),
            duplicidade_corrigido.col("tabela"),
            duplicidade_corrigido.col("dt_foto"),
            duplicidade_corrigido.col("var_nome_campo"),
            duplicidade_corrigido.col("var_formato_dt_foto"),
            duplicidade_corrigido.col("status")
          )
        )
        .dropDuplicates
        .orderBy("dt_foto")

      dq_duplicidade_falhas.show

      dq_duplicidade_falhas.registerTempTable("dq_duplicidade_falhas_temp")

      val droprow = sqlContext.sql(
        s"""
          insert overwrite table h_bigd_dq_db.dq_duplicidade_falhas
          select  * from dq_duplicidade_falhas_temp
         """)

    }

  }

}
