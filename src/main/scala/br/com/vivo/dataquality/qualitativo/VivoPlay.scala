package br.com.vivo.dataquality.qualitativo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object VivoPlay extends App {

  val var_dt_foto: String = args(0)

  /**
  | Example command line to run this app:
    time spark-submit --master yarn  \
   --queue Qualidade \
   --class br.com.vivo.dataquality.qualitativo.VivoPlay  \
    /home/SPDQ/indra/dataquality_2.10-0.1.jar 20210707
   */

  val sc = new SparkContext(new SparkConf() )
  val sqlContext = new HiveContext(sc)

  val sqlVivoPlay = sqlContext.sql(
    s"""
       |
       |-- HISTORICO DO CUBO
       |select
       |A.banco,
       |A.tabela,
       |A.dt_foto,
       |A.valor_faixa,
       |A.valor_faixa_qtde,
       |A.usuarios_unicos,
       |A.transacoes_unicas,
       |A.valor_total,
       |A.valor_medio_usuario,
       |A.valor_medio_transacao,
       |A.usuarios_unicos_12299,
       |A.transacoes_unicas_12299,
       |A.valor_total_12299,
       |A.valor_medio_usuario_12299,
       |A.valor_medio_transacao_12299,
       |A.usuarios_unicos_9790,
       |A.transacoes_unicas_9790,
       |A.valor_total_9790,
       |A.valor_medio_usuario_9790,
       |A.valor_medio_transacao_9790,
       |A.usuarios_unicos_8990,
       |A.transacoes_unicas_8990,
       |A.valor_total_8990,
       |A.valor_medio_usuario_8990,
       |A.valor_medio_transacao_8990,
       |A.usuarios_unicos_050,
       |A.transacoes_unicas_050,
       |A.valor_total_050,
       |A.valor_medio_usuario_050,
       |A.valor_medio_transacao_050,
       |A.dt_processamento,
       |'1' as fonte
       |from h_bigd_dq_db.dq_qualitativo1_medidas as A
       |where
       |-- FILTRO DT_FOTO + DT_PROCESSAMENTO
       |concat (
       |   A.dt_foto,
       |   A.dt_processamento
       |)
       |   <>
       |concat (
       |   '${var_dt_foto}',
       |   cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string)
       |)
       |
       |----------
       |union all
       |----------
       |
       |-- TABELA 1: p_bigd_tv_db.tbgdt_tv_vw_api_ppv_cmpr_vivo_play
       |select
       |A2.banco,
       |A2.tabela,
       |A2.dt_foto,
       |A2.valor_faixa,
       |count(A2.valor_faixa) valor_faixa_qtde,
       |
       |-- medidas gerais
       |count(distinct A2.uniqueid) as usuarios_unicos,
       |count(distinct A2.codigo_unico_transacao) as transacoes_unicas,
       |sum(A2.valor) as valor_total,
       |---
       |cast( (sum(A2.valor) / count(distinct A2.uniqueid)              ) as decimal(20,2)) as valor_medio_usuario,
       |cast( (sum(A2.valor) / count(distinct A2.codigo_unico_transacao)) as decimal(20,2)) as valor_medio_transacao,
       |
       |
       |-- medidas valor 122.99
       |count(distinct case when A2.valor = 122.99 then A2.uniqueid end)               as usuarios_unicos_12299,
       |count(distinct case when A2.valor = 122.99 then A2.codigo_unico_transacao end) as transacoes_unicas_12299,
       |sum(case when A2.valor = 122.99 then A2.valor end)                             as valor_total_12299,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 122.99 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 122.99 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_12299,
       |---
       |cast(
       |    sum(case when A2.valor = 122.99 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 122.99 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_12299,
       |
       |
       |-- medidas valor 97.90
       |count(distinct case when A2.valor = 97.90 then A2.uniqueid end)               as usuarios_unicos_9790,
       |count(distinct case when A2.valor = 97.90 then A2.codigo_unico_transacao end) as transacoes_unicas_9790,
       |sum(case when A2.valor = 97.90 then A2.valor end)                             as valor_total_9790,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 97.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 97.90 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_9790,
       |---
       |cast(
       |    sum(case when A2.valor = 97.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 97.90 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_9790,
       |
       |
       |-- medidas valor 89.90
       |count(distinct case when A2.valor = 89.90 then A2.uniqueid end)               as usuarios_unicos_8990,
       |count(distinct case when A2.valor = 89.90 then A2.codigo_unico_transacao end) as transacoes_unicas_8990,
       |sum(case when A2.valor = 89.90 then A2.valor end)                             as valor_total_8990,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 89.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 89.90 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_8990,
       |---
       |cast(
       |    sum(case when A2.valor = 89.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 89.90 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_8990,
       |
       |
       |-- medidas valor 0.50
       |count(distinct case when A2.valor = 0.50 then A2.uniqueid end)               as usuarios_unicos_050,
       |count(distinct case when A2.valor = 0.50 then A2.codigo_unico_transacao end) as transacoes_unicas_050,
       |sum(case when A2.valor = 0.50 then A2.valor end)                             as valor_total_050,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 0.50 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 0.50 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_050,
       |---
       |cast(
       |    sum(case when A2.valor = 0.50 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 0.50 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_050,
       |
       |cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string) dt_processamento,
       |'2' as fonte
       |from (
       |   select
       |   'p_bigd_tv_db' as banco,
       |   'tbgdt_tv_vw_api_ppv_cmpr_vivo_play' as tabela,
       |   cast(from_unixtime(unix_timestamp(A1.dt_foto,'yyyy-MM-dd'),'yyyyMMdd') as string) dt_foto,
       |   A1.uniqueid,
       |   A1.codigo_unico_transacao,
       |   cast(valor as decimal(30,2)) valor,
       |   case
       |      when cast(A1.valor as decimal(30,2))  = 0   then '=0'
       |      when cast(A1.valor as decimal(30,2)) <= 1   then '0-1'
       |      when cast(A1.valor as decimal(30,2)) <= 3   then '2-3'
       |      when cast(A1.valor as decimal(30,2)) <= 5   then '4-5'
       |      when cast(A1.valor as decimal(30,2)) <= 8   then '5-8'
       |      when cast(A1.valor as decimal(30,2)) <= 10  then '8-10'
       |      when cast(A1.valor as decimal(30,2)) <= 12  then '10-12'
       |      when cast(A1.valor as decimal(30,2)) <= 15  then '12-15'
       |      when cast(A1.valor as decimal(30,2)) <= 20  then '15-20'
       |      when cast(A1.valor as decimal(30,2)) <= 50  then '20-50'
       |      when cast(A1.valor as decimal(30,2)) <= 100 then '50-100'
       |      when cast(A1.valor as decimal(30,2)) >  100 then '100-999'
       |   end valor_faixa
       |   from p_bigd_tv_db.tbgdt_tv_vw_api_ppv_cmpr_vivo_play as A1
       |   where cast(from_unixtime(unix_timestamp(A1.dt_foto,'yyyy-MM-dd'),'yyyyMMdd') as string) = '${var_dt_foto}'
       |) as A2
       |
       |group by
       |A2.banco,
       |A2.tabela,
       |A2.dt_foto,
       |A2.valor_faixa
       |
       |
       |---
       |UNION ALL
       |---
       |
       |
       |-- TABELA 2: p_bigd_tv_db.tbgdt_tv_vw_api_cmpr_vivo_play
       |select
       |A2.banco,
       |A2.tabela,
       |A2.dt_foto,
       |A2.valor_faixa,
       |count(A2.valor_faixa) valor_faixa_qtde,
       |
       |-- medidas gerais
       |count(distinct A2.uniqueid) as usuarios_unicos,
       |count(distinct A2.codigo_unico_transacao) as transacoes_unicas,
       |sum(A2.valor) as valor_total,
       |---
       |cast( (sum(A2.valor) / count(distinct A2.uniqueid)              ) as decimal(20,2)) as valor_medio_usuario,
       |cast( (sum(A2.valor) / count(distinct A2.codigo_unico_transacao)) as decimal(20,2)) as valor_medio_transacao,
       |
       |
       |-- medidas valor 122.99
       |count(distinct case when A2.valor = 122.99 then A2.uniqueid end)               as usuarios_unicos_12299,
       |count(distinct case when A2.valor = 122.99 then A2.codigo_unico_transacao end) as transacoes_unicas_12299,
       |sum(case when A2.valor = 122.99 then A2.valor end)                             as valor_total_12299,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 122.99 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 122.99 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_12299,
       |---
       |cast(
       |    sum(case when A2.valor = 122.99 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 122.99 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_12299,
       |
       |
       |-- medidas valor 97.90
       |count(distinct case when A2.valor = 97.90 then A2.uniqueid end)               as usuarios_unicos_9790,
       |count(distinct case when A2.valor = 97.90 then A2.codigo_unico_transacao end) as transacoes_unicas_9790,
       |sum(case when A2.valor = 97.90 then A2.valor end)                             as valor_total_9790,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 97.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 97.90 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_9790,
       |---
       |cast(
       |    sum(case when A2.valor = 97.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 97.90 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_9790,
       |
       |
       |-- medidas valor 89.90
       |count(distinct case when A2.valor = 89.90 then A2.uniqueid end)               as usuarios_unicos_8990,
       |count(distinct case when A2.valor = 89.90 then A2.codigo_unico_transacao end) as transacoes_unicas_8990,
       |sum(case when A2.valor = 89.90 then A2.valor end)                             as valor_total_8990,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 89.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 89.90 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_8990,
       |---
       |cast(
       |    sum(case when A2.valor = 89.90 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 89.90 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_8990,
       |
       |
       |-- medidas valor 0.50
       |count(distinct case when A2.valor = 0.50 then A2.uniqueid end)               as usuarios_unicos_050,
       |count(distinct case when A2.valor = 0.50 then A2.codigo_unico_transacao end) as transacoes_unicas_050,
       |sum(case when A2.valor = 0.50 then A2.valor end)                             as valor_total_050,
       |--- ---
       |cast(
       |    sum(case when A2.valor = 0.50 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 0.50 then A2.uniqueid end)
       |as decimal(20,2)) valor_medio_usuario_050,
       |---
       |cast(
       |    sum(case when A2.valor = 0.50 then A2.valor end)
       |    /
       |    count(distinct case when A2.valor = 0.50 then A2.codigo_unico_transacao end)
       |as decimal(20,2)) valor_medio_transacao_050,
       |
       |cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string) dt_processamento,
       |'3' as fonte
       |
       |from (
       |   select
       |   'p_bigd_tv_db' as banco,
       |   'tbgdt_tv_vw_api_cmpr_vivo_play' as tabela,
       |   cast(from_unixtime(unix_timestamp(A1.dt_foto,'yyyy-MM-dd'),'yyyyMMdd') as string) dt_foto,
       |   A1.uniqueid,
       |   A1.codigo_unico_transacao,
       |   cast(preco as decimal(30,2)) valor,
       |   case
       |      when cast(A1.preco as decimal(30,2))  = 0   then '=0'
       |      when cast(A1.preco as decimal(30,2)) <= 1   then '0-1'
       |      when cast(A1.preco as decimal(30,2)) <= 3   then '2-3'
       |      when cast(A1.preco as decimal(30,2)) <= 5   then '4-5'
       |      when cast(A1.preco as decimal(30,2)) <= 8   then '5-8'
       |      when cast(A1.preco as decimal(30,2)) <= 10  then '8-10'
       |      when cast(A1.preco as decimal(30,2)) <= 12  then '10-12'
       |      when cast(A1.preco as decimal(30,2)) <= 15  then '12-15'
       |      when cast(A1.preco as decimal(30,2)) <= 20  then '15-20'
       |      when cast(A1.preco as decimal(30,2)) <= 50  then '20-50'
       |      when cast(A1.preco as decimal(30,2)) <= 100 then '50-100'
       |      when cast(A1.preco as decimal(30,2)) >  100 then '100-999'
       |   end valor_faixa
       |   from p_bigd_tv_db.tbgdt_tv_vw_api_cmpr_vivo_play as A1
       |   where cast(from_unixtime(unix_timestamp(A1.dt_foto,'yyyy-MM-dd'),'yyyyMMdd') as string) = '${var_dt_foto}'
       |) as A2
       |
       |group by
       |A2.banco,
       |A2.tabela,
       |A2.dt_foto,
       |A2.valor_faixa
       |
       |---
       |order by
       |banco,
       |tabela,
       |dt_foto,
       |valor_faixa
       |
       |
       |""".stripMargin).show()

  sc.stop()

}
