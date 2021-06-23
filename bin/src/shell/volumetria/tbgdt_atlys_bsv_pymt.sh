#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

 time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 4g \
--executor-cores 10 \
--num-executors 100 \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria \
$jar_executor p_bigd_db tbgdt_atlys_bsv_pymt "$data_ontem" dt_foto 1

