#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 40g \
--executor-cores 15 \
--num-executors 10 \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria \
$jar_executor p_bigd_urm tbgd_turm_bill "$data_ontem" dt_foto 1

