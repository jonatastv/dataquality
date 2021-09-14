#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue ${queue} \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
$jar_executor p_bigd_tv_db tbgdt_tv_tb_wrk_vod_plays_tv_v2 "$data_ontem" dt_foto 2 
#&> "$dir_log_duplicidade"/tbgdt_tv_tb_wrk_vod_plays_tv_v2.txt


