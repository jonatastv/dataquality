#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_6d="$(date -d "-6 day 13:00" '+%Y%m%d')"

time spark-submit --master yarn  \
--queue ${queue} \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria \
$jar_executor p_bigd_urm tbgd_turm_lgpd "$data_6d" dt_foto 1

