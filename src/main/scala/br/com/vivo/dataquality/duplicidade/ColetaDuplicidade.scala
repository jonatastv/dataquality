package br.com.vivo.dataquality.duplicidade
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import scala.util.{Failure, Success, Try}
import java.io.{File, PrintWriter, StringWriter}
import org.apache.spark.sql.types._


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

 CREATE TABLE IF NOT EXISTS h_bigd_dq_db.temp_dtfoto_teste ( dt_foto String, valor String)
STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');

   */

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)
  val erroutput = new StringWriter


 // val tabela = h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_ + ${database} + "_"+${table}+"_teste"
  try {

    sqlContext.setConf("hive.exec.dynamic.partition.mode" ,"nonstrict")
    sqlContext.setConf("hive.exec.dynamic.partition" ,"true")

 val partiton_df = sqlContext.sql(s"show partitions ${database}.${table}").toDF("result")

  partiton_df.orderBy(desc("result")).show()

  partiton_df.registerTempTable("partitions_df")

 val ff = sqlContext.sql(
   s"""
       select result from partitions_df
       where
       case
       when '$var_formato_dt_foto' = '1' then cast(result as string) = '$var_nome_campo=$var_data_foto'
       when '$var_formato_dt_foto' = '2' then date_format(regexp_replace(result, '$var_nome_campo=',''),"yyyyMMdd") = "$var_data_foto"
       end
       """).count()

  println(ff)

  if (ff == 0) {
    println("não existe partição para essa dt_foto "+ff)

    val save_df = sqlContext.sql(
      s"""
         |select '$database' as banco,
         |'$table' as tabela,
         |'$var_data_foto' as dt_foto,
         | date_format(current_date(),"yyyyMMdd") as dt_processamento,
         |'0' as status
         |""".stripMargin)

      save_df.
      write.
      mode("append").
      format("orc").
      insertInto("h_bigd_dq_db.temp_dtfoto_teste")

            }
  else {
    println("tem partição " +ff )


  val dropDF = sqlContext.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste")

    val duplicateDF = sqlContext.sql(
      s"""

  create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste
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

    //duplicateDF.show

   // duplicateDF.registerTempTable("duplicados_medidas")



  /*  val dropDF = sqlContext.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste")
    val createDF = sqlContext.sql(
      s"""create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste
         |STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
         |select * from duplicados_medidas""".stripMargin)
*/


  /*  val ff = partiton_df.select("*")
     // .where(s"result = '$var_nome_campo=$var_data_foto'")
    //  .where(s"result = 'dt_foto=2021-06-07'")
      .where(
        s"""
           |case
           |            when '$var_formato_dt_foto' = '1' then cast(result as string) = '$var_nome_campo=$var_data_foto'
           |            when '$var_formato_dt_foto' = '2' then cast(date_format(result,"yyyyMMdd") as string) = '$var_nome_campo=$var_data_foto'
           |end""".stripMargin)

      .count()
  */






/*
    val df_sucess = sqlContext.table(s"h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste")
    df_sucess.select(
      df_sucess.col("banco"),
      df_sucess.col("tabela"),
      df_sucess.col("dt_foto"),
      df_sucess.col("dt_processamento")
    ).
      withColumn("status", lit(1))
*/

    // status 1 para sucesso ao carregar partição dt_foto da tabela

 //h_bigd_dq_db.dq_duplicados_medidas
  /*  val medidasDF  = sqlContext.table("h_bigd_dq_db.temp_medidas_teste").alias("A")
      .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde1","A.qtde2","A.diferenca")
      .withColumn("fonte",lit(1))
       .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}', date_format(current_date(),"yyyyMMdd") )""")
    //  .where(col("banco") !== database)
    //  .where(col("tabela") !== table)
    //  .where(col("dt_foto") !== var_data_foto)
    //  .where(col("dt_processamento") !== date_format(current_date(),"yyyyMMdd"))
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
      ).dropDuplicates
      .toDF()
*/

/*
    medidasDF.registerTempTable("duplicados_medidas")
    val dropf = sqlContext.sql("drop table if exists h_bigd_dq_db.temp_medidas_teste")
    val final_medidas = sqlContext.sql(
      s"""
         |create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_teste
         |STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
         |select distinct * from duplicados_medidas""".stripMargin)
*/


 //   val createMedidas = sqlContext.sql(s"create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_teste")
/*
    duplicateDF.
      write.
      mode("append").
      format("orc").
      insertInto("h_bigd_dq_db.temp_medidas_teste")
      //h_bigd_dq_db.dq_duplicados_medidas
    Success(duplicateDF)
*/
  //  val tempview = sqlContext.sql(s"select '$var_data_foto' as dt_foto , '1' as valor ")
    // valor 1 para sucesso ao carregar partição dt_foto da tabela

  /*  df_sucess.
      write.
      mode("append").
      format("orc").
      insertInto("h_bigd_dq_db.temp_dtfoto_teste")
*/

  }

  } catch
  {
    case _: Throwable => val temperror = sqlContext.sql(
      s"""
         | -- insert overwrite table h_bigd_dq_db.temp_dtfoto_teste
         |select distinct  '$database' as banco,
         |'$table' as tabela,
         |'$var_data_foto' as dt_foto,
         | date_format(current_date(),"yyyyMMdd") as dt_processamento,
         |'0' as status
         |""".stripMargin)

      temperror.
        distinct().
        write.
        mode("append").
        format("orc").
        insertInto("h_bigd_dq_db.temp_dtfoto_teste")

      /*val ff = sqlContext.sql(
      s"""
         create Table IF NOT EXISTS h_bigd_dq_db.temp_dtfoto_teste

          STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
          select * from teste

         """)
      **/
    /*d.printStackTrace(new PrintWriter(erroutput))
      new PrintWriter(s"/tmp/yspark_log") //Saves error message to this location
      {
        write(erroutput.toString);

        close
      }
      */
  }



}
