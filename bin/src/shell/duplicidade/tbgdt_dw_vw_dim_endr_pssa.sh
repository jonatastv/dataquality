#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_2d="$(date -d "-2 day 13:00" '+%Y%m%d')"

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 6g \
--executor-cores 10 \
--num-executors 3 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
$jar_executor p_bigd_db tbgdt_dw_vw_dim_endr_pssa "$data_2d" dt_foto 1

