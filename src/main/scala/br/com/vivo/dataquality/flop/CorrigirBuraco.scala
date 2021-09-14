package br.com.vivo.dataquality.flop

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CorrigirBuraco extends  App{


  val atualizacao: String = args(0)


  /**
  | Example command line to run this app:
  export SPARK_MAJOR_VERSION=2
   | time spark-submit \
  --master yarn \
  --executor-memory 27g \
  --driver-memory 3g \
  --queue Qualidade \
  --class br.com.vivo.dataquality.flop.CorrigirBuraco \
  /home/SPDQ/indra/dataquality-master_2.11-1.1.jar diario

   */

  val spark = SparkSession
    .builder()
    .appName(s"corrigir_Volumetria")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()
  //sc.setLogLevel("ERROR")

  def criarDataframe(frequencia: String): DataFrame = {
    if (frequencia == "diario") {

      val df_diario = spark.sql(
        s"""
       select distinct *
       from h_bigd_dq_db.dq_volumetria_falhas
       where status = 0
       and tabela not in ('tbgd_turm_customer')
       and dt_foto >= date_format(date_add(current_date,-1),"yyyyMMdd")

       """).toDF()
      return df_diario
    }
    else  {

      val df_semanal  = spark.sql(
        s"""
    select distinct *
      from h_bigd_dq_db.dq_volumetria_falhas
     where tabela not in ('tbgd_turm_customer')
       and status = 0
       and dt_foto between date_format(date_add(current_date,-7),"yyyyMMdd") and date_format(date_add(current_date,-2),"yyyyMMdd")

       """).toDF()
      return df_semanal
    }

  }


  val dfsql = criarDataframe(s"${atualizacao}")
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

  /*
val s = ""
  for((x,i) <- tiposTabela.view.zipWithIndex) {
    println("String #" + i + " is " + x)

  }
*/

  for( wi <-  database.indices ){
    println("value #" + wi + " is " + database(wi))
    println("value #" + wi + " is " + table(wi))


    val partiton_df = spark.sql(s"show partitions ${database(wi)}.${table(wi)}").toDF("result")

    partiton_df.orderBy(desc("result")).show()

    partiton_df.registerTempTable("partitions_df")

    val ff = spark.sql(
      s"""
       select result from partitions_df
       where
       case
       when '${var_formato_dt_foto(wi)}' = '1' then cast(result as string) = '${var_nome_campo(wi)}=${var_data_foto(wi)}'
       when '${var_formato_dt_foto(wi)}' = '2' then date_format(regexp_replace(result, '${var_nome_campo(wi)}=',''),"yyyyMMdd") = "${var_data_foto(wi)}"
       end
       """).count()

    println(ff)

    if (ff == 0) {
      println("não existe partição para essa dt_foto "+ff)


    }
    else {

      val parametrosDf = spark.sql(
        s"""
           |select disponibilidade_fonte ,
           |disponibilidade_detalhe ,
           |tabela_medida
           |from h_bigd_dq_db.dq_parametros
           |where tabela = '${table(wi)}'
           |""".stripMargin)

      val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
        projeto_2.getString(0).toLowerCase
      }

      var projetos = ""
      for( i <-  projeto.indices ) {
        projetos = projeto(i)
      }

      val tempDF = spark.sql(
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

      val volumetria_medidas = spark.table(s"h_bigd_dq_db.dq_volumetria_medidas${projetos}").as("A")
        .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
        .where(s"""concat(A.banco, A.tabela, A.dt_foto,  A.dt_processamento) <> concat('${database(wi)}','${table(wi)}','${var_data_foto(wi)}', date_format(current_date(),"yyyyMMdd") )""")
        .withColumn("fonte",lit(2))
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
      val final_medidas = spark.sql(
        s"""
           |-- create Table IF NOT EXISTS h_bigd_dq_db.temp_medidas_volumetria_teste
           |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
           |insert overwrite table h_bigd_dq_db.dq_volumetria_medidas${projetos}
           |select  * from volumetria_medidas""".stripMargin)

      println("passou aqui!")
      val volumetria_corrigido = spark.sql(
        s"""
           |select
           |'${database(wi)}' as banco,
           |'${table(wi)}' as tabela,
           |'${var_data_foto(wi)}' as dt_foto,
           |'${var_nome_campo(wi)}' as var_nome_campo,
           |'${var_formato_dt_foto(wi)}' as var_formato_dt_foto,
           |1 as status
           |
           |""".stripMargin)

      val volumetria = spark.sql(
        s"""
           |select  *
           |from h_bigd_dq_db.dq_volumetria_falhas
           |where
           |tabela not in ('tbgd_turm_customer')
           |""".stripMargin)

      val dq_volumetria_falhas = volumetria.select("banco","tabela","dt_foto","var_nome_campo","var_formato_dt_foto","status")
        .where(s"""concat(banco, tabela, dt_foto) <> concat('${database(wi)}','${table(wi)}','${var_data_foto(wi)}')""")
        .unionAll(
          volumetria_corrigido.select(
            volumetria_corrigido.col("banco"),
            volumetria_corrigido.col("tabela"),
            volumetria_corrigido.col("dt_foto"),
            volumetria_corrigido.col("var_nome_campo"),
            volumetria_corrigido.col("var_formato_dt_foto"),
            volumetria_corrigido.col("status")
          )
        )
        .dropDuplicates
        .orderBy("dt_foto")

      dq_volumetria_falhas.show

      dq_volumetria_falhas.registerTempTable("dq_volumetria_falhas_temp")

      val droprow = spark.sql(
        s"""
          insert overwrite table h_bigd_dq_db.dq_volumetria_falhas
          select distinct * from dq_volumetria_falhas_temp
         """)


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
  
    val ff = spark.sql(
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
