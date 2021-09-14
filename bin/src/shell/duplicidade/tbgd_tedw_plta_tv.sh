#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_4d="$(date -d "-4 day 13:00" '+%Y%m%d')"

time spark-submit --master yarn  \
--queue ${queue} \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
$jar_executor p_bigd_db tbgd_tedw_plta_tv "$data_4d" dt_foto_bigdata 1

