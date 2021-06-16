package br.com.vivo.dataquality.flop

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object CorrigirBuraco extends  App{

  /**
  | Example command line to run this app:
   | time spark-submit --master yarn  \
   | --queue Qualidade \
   | --class br.com.vivo.dataquality.duplicidade.JuntaTabela \
   | /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money 20210520 dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste

   */

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)
  sc.setLogLevel("ERROR")




  val dfsql = sqlContext.sql(
    s"""
       select distinct *
       from h_bigd_dq_db.temp_dtfoto_teste
       where dt_foto = '20210615'
       and tabela not in ('com_amdocs_insight_dh_shared_mutables_payment_paymentmutable','tbgd_turm_customer')

       """).toDF()

  dfsql.show()

  //tabela.foreach(f=> println(f))

  //for ((name,rating) < tabela.) println(s"Movie: $name, Rating: $rating")

  val database: Array[String] = for (database <- dfsql.select("banco").collect()) yield {
    database.getString(0)

  }

  val table: Array[String] = for (table <- dfsql.select("tabela").collect()) yield {
    table.getString(0)

  }

  val var_data_foto: Array[String] = for (var_data_foto <- dfsql.select("dt_foto").collect()) yield {
    var_data_foto.getString(0)

  }


  /*
val s = ""
  for((x,i) <- tiposTabela.view.zipWithIndex) {
    println("String #" + i + " is " + x)

  }
*/

  for( wi <-  database.indices ){
    println("value #" + wi + " is " + database(wi))
    println("value #" + wi + " is " + table(wi))


    val partiton_df = sqlContext.sql(s"show partitions ${database(wi)}.${table(wi)}").toDF("result")

    partiton_df.orderBy(desc("result")).show()

    partiton_df.registerTempTable("partitions_df")

    val ff = sqlContext.sql(
      s"""
       select result from partitions_df
       where
       result = 'dt_foto=${var_data_foto(wi)}'
       """).count()

    println(ff)

    if (ff == 0) {
      println("não existe partição para essa dt_foto "+ff)


    }
    else {

    val tempDF = sqlContext.sql(
      s"""
         |select
         |'${database(wi)}' as banco,
         |'${table(wi)}' as tabela,
         |'${var_data_foto(wi)}' as dt_foto,
         |date_format(current_date(),"yyyyMMdd") as dt_processamento,
         |A2.qtde_registros,
         |'1' as fonte
         |from (
         |select count(*) as qtde_registros from ${database(wi)}.${table(wi)}
         |where
         |dt_foto = '${var_data_foto(wi)}'
         | ) as A2
         |""".stripMargin).toDF()
    tempDF.show()

    val volumetria_medidas = sqlContext.table("h_bigd_dq_db.temp_medidas_volumetria_teste").as("A")
      .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
      //.where(s"""concat(A.banco, A.tabela, A.dt_foto, A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}',cast(date_format(current_date(),"yyyyMMdd") as string)""")
      .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database(wi)}','${table(wi)}','${var_data_foto(wi)}', date_format(current_date(),"yyyyMMdd") )""")
      // .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database}','${table}','${var_data_foto}', date_format(current_date(),"yyyyMMdd") )""")
      .withColumn("fonte",lit(1))
      .unionAll(
        tempDF.select(
          tempDF.col("banco"),
          tempDF.col("tabela"),
          tempDF.col("dt_foto"),
          tempDF.col("dt_processamento"),
          tempDF.col("qtde_registros"),
          tempDF.col("fonte")
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

    /*
    val droprow = sqlContext.sql(
      s"""update  h_bigd_dq_db.temp_medidas_volumetria_teste set status = '1' where banco = ${database(wi)}
         and tabela = ${table(wi)}
         and dt_foto = ${var_data_foto(wi)} """)
*/

  }
    }
/*
  var i = 0
  while ( {
    i < tiposTabela.length
  }) {
    println("String #" + i + " is " + tiposTabela(i))
    println("String #" + i + " is " + tiposHub(i))

    i += 1
  }

*/



  /*
   for (i <- 1 to 6){
     println(tiposHub.indexOf(i))

   }
 */

 // tiposHub.foreach(e => println(e))

  //tabela.show
  //tabela.foreach(f=> println(f))
  //val database = tabela.col("banco")


/*

  val ff = sqlContext.sql(
    s"""
       |select distinct * from $database.$table
       |      -- filtrando a dt_foto passada
       |      where
       |      dt_foto = '$var_data_foto'
       |
       |""".stripMargin)

*/


  //tabela.foreach(f=> println(f))

}
