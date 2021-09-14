package br.com.vivo.dataquality.duplicidade

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object teste extends App{


  val database: String = args(0)
  val table: String = args(1)
  val var_nome_campo: String = args(2)
 // val var_data_foto: String = args(2)
//  val var_nome_campo: String = args(3)
 // val var_formato_dt_foto: String = args(4)

  val spark = SparkSession
    .builder()
    .appName(s"corrigir_Duplicidade")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

  val df_diario = spark.sql(
    s"""
      SELECT * from p_bigd_urm.tbgd_turm_bill where dt_foto = '20210815'

       """).toDF().dropDuplicates()

  val df1 = df_diario.groupBy("dt_foto").count().show()


  /*val df1 = df_diario.withColumn("hash_value",sha2(concat(df_diario.columns.map(col):_*),256))
  df1.groupBy(s"${var_nome_campo}","hash_value").count().orderBy(
    desc(s"${var_nome_campo}")).withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
  df1.registerTempTable("teste")
*/
 // val ff = spark.sql("select count(total) from (select count(hash_value) as total, hash_value from teste group by hash_value having count(hash_value) > 1 ) as f ").show()

//  val df2 = df_diario.withColumn("rowsha",sha2(df_diario.columns.foldLeft(lit(""))((x,y)=>concat(x,coalesce(col(y),lit("n/a")))),256))


//  val ffd = df_diario.select("customer_id")
 //   .agg(countDistinct("customer_id"))
  //  .show()
System.exit(1)

  /*
  val df2 =  df_diario.withColumn("rowsha",sha2(df_diario.columns.foldLeft(lit(""))((x,y)=>concat(x,coalesce(col(y),lit("n/a")))),256))
  //  df2.groupBy("rowsha").agg(count("rowsha").alias("count"))
    //  .filter(col("count")>1)
    .show()
  //    .registerTempTable("teste2")

 // val df3 = df_diario.withColumn("rowsha",sha2(df_diario.columns.foldLeft(lit(""))((x,y)=>concat(x,col(y))),256))

  val df3 =  df_diario.withColumn("rowsha",sha2(df_diario.columns.foldLeft(lit(""))((x,y)=>concat(x,coalesce(col(y),lit("n/a")))),256))
  df3.groupBy("rowsha").agg(count("rowsha").alias("count"))
    .show()
  //  .createOrReplaceTempView("teste3")
*/

/*
  val ff2 = spark.sql(
    """
      |select
      |cast(B2.rowsha as bigint) qtde1,
      |cast(C2.rowsha as bigint) qtde2,
      |cast(B2.rowsha as bigint) - cast(C2.rowsha as bigint) diferenca
      |
      |from (
      |   -- GRUPO 1
      |   select
      |   date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
      |   date_format(current_date(),"yyyyMMdd") as dt_processamento
      |) as A2
      |
      |left join (
      |
      |select
      |date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
      |count(rowsha) as rowsha from teste2
      |) as B2
      |
      |on B2.dt_foto = A2.dt_foto
      |
      |left join (
      |select date_format(date_sub(current_date() , 1 ),"yyyyMMdd")  as dt_foto,
      |count(rowsha) as rowsha from teste3
      |) as C2
      |
      |on C2.dt_foto = A2.dt_foto
      |""".stripMargin).show()

*/

}
