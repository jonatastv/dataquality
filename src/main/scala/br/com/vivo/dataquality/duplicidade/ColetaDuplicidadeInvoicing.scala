package br.com.vivo.dataquality.duplicidade


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColetaDuplicidadeInvoicing extends  App {

  val database: String = args(0)
  val table: String = args(1)

  /**
   *    time spark-submit --master yarn  \
    --queue Qualidade \
    --class br.com.vivo.dataquality.duplicidade.ColetaDuplicidadeInvoicing \
    /home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money
   *
   */
  val spark = SparkSession
    .builder()
    .appName(s"Duplicidade_${table}")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.port.maxRetries", "100")
    .enableHiveSupport()
    .getOrCreate()

  val dropDF = spark.sql(s"drop table if exists h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t")

  val duplicateDF = spark.sql(
    s"""

  create Table IF NOT EXISTS h_bigd_dq_db.dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t
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
   select distinct
   '$database' as banco,
   '$table' as tabela,
   date_format(substring(A1.p_dataset_date,1,10),"yyyyMMdd") dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento
   from ${database}.${table} A1
) as A2

left join (
  select
  date_format(substring(B1.p_dataset_date,1,10),"yyyyMMdd") dt_foto,
  date_format(current_date(),"yyyyMMdd") as dt_processamento,
  count(B1.p_dataset_date) qtde1
  from $database.$table as B1

   -- filtrando a dt_foto passada
   where B1.p_state = 'published'
   group by
   date_format(substring(B1.p_dataset_date,1,10),"yyyyMMdd")
   LIMIT 2

) as B2
   on B2.dt_foto = A2.dt_foto
  and B2.dt_processamento = A2.dt_processamento

left join (
   select
   date_format(substring(C1.p_dataset_date,1,10),"yyyyMMdd") dt_foto,
   date_format(current_date(),"yyyyMMdd") as dt_processamento,
   count(C1.p_dataset_date) qtde2
  from (
select distinct * from $database.$table
 where p_state = 'published'


 ) as C1

group by
date_format(substring(C1.p_dataset_date,1,10),"yyyyMMdd")

)  as C2
   on C2.dt_foto = A2.dt_foto
  and C2.dt_processamento = A2.dt_processamento
    """)


}
