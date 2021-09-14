#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_3d="$(date -d "-3 day 13:00" '+%Y%m%d')"

time spark-submit --master yarn  \
--queue ${queue} \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria \
$jar_executor p_bigd_db tbgd_tedw_hstr_pssa "$data_3d" dt_foto 1

