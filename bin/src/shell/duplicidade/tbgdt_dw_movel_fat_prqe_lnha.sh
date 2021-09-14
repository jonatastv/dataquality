#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_5d="$(date -d "-5 day 13:00" '+%Y%m%d')"

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 4g \
--executor-cores 10 \
--num-executors 10 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
$jar_executor p_bigd_db tbgdt_dw_movel_fat_prqe_lnha "$data_5d" dt_foto_lnha 2

