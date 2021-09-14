package br.com.vivo.dataquality.volumetria


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColetaVolumetriaInvoicing extends  App {

  val database: String = args(0)
  val table: String = args(1)


  val spark = SparkSession
    .builder()
    .appName(s"Volumetria_$table")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

  val parametrosDf = spark.sql(
    s"""
       |select disponibilidade_fonte ,
       |disponibilidade_detalhe ,
       |tabela_medida
       |from h_bigd_dq_db.dq_parametros
       |where tabela = '${table}'
       |""".stripMargin)


  val projeto: Array[String] = for (projeto_2 <- parametrosDf.select("tabela_medida").collect()) yield {
    projeto_2.getString(0).toLowerCase
  }

  for( i <-  projeto.indices ) {


  val tabela = spark.sql(
    s"""
       |select
       |'${database}' as banco,
       |'${table}' as tabela,
       |A2.dt_foto,
       |date_format(current_date(),"yyyyMMdd") as dt_processamento,
       |A2.qtde_registros,
       |'2' as fonte
       |from (
       |select
       |date_format(substring(p_dataset_date,1,10),"yyyyMMdd") dt_foto,
       |count(*) as qtde_registros
       |from ${database}.${table}
       |group by date_format(substring(p_dataset_date,1,10),"yyyyMMdd")
       | ) as A2
       |""".stripMargin).toDF()

  //val tabela = spark.sql(s"""select p_dataset_date , date_format(substring(p_dataset_date,1,10),"yyyyMMdd")  from ${database}.${table}""")
  tabela.show

  // System.exit(1)


  val volumetria_medidas = spark.table(s"h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}").as("A")
    .select("A.banco","A.tabela","A.dt_foto","A.dt_processamento","A.qtde_registros")
    .where(s"""concat(A.banco, A.tabela,  A.dt_processamento) <> concat('${database}','${table}', date_format(current_date(),"yyyyMMdd") )""")
    .withColumn("fonte",lit(1))
    .unionAll(
      tabela.select(
        tabela.col("banco"),
        tabela.col("tabela"),
        tabela.col("dt_foto"),
        tabela.col("dt_processamento"),
        tabela.col("qtde_registros"),
        tabela.col("fonte")
      )
    )
    .dropDuplicates
    .orderBy("banco","tabela","dt_foto","dt_processamento")


  volumetria_medidas.createOrReplaceTempView("volumetria_medidas")
  val final_medidas = spark.sql(
    s"""
       |-- create Table IF NOT EXISTS h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}
       |-- STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY') as
       |insert overwrite table h_bigd_dq_db.dq_volumetria_medidas${projeto(i)}
       |select  * from volumetria_medidas""".stripMargin)

  }

}
