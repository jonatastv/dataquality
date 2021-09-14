#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`


source "$CONF_GLOBAL_FILE"

#data=$(date '+%Y-%m-%d %H:%M:%S')

log_file="tbgd_turm_customer.txt"

#echo '' >> "${dir_log_volumetria}/${log_file}"

 time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 40g \
--executor-cores 10 \
--num-executors 3 \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria \
$jar_executor p_bigd_urm tbgd_turm_customer "$data_ontem" dt_foto 1 
#> "${dir_log_volumetria}/${log_file}"

