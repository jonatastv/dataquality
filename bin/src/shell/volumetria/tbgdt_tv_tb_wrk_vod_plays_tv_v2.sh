#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 40g \
--executor-cores 10 \
--num-executors 3 \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria \
$jar_executor p_bigd_tv_db tbgdt_tv_tb_wrk_vod_plays_tv_v2 "$data_ontem" dt_foto 2

