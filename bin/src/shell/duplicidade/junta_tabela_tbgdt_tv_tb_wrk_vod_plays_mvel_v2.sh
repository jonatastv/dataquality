#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

database='p_bigd_tv_db'

table='tbgdt_tv_tb_wrk_vod_plays_mvel_v2'

time spark-submit --master yarn  \
--queue ${queue} \
--class br.com.vivo.dataquality.duplicidade.JuntaTabela \
$jar_executor ${database} ${table} "$data_ontem" dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste
