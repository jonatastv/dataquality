#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue ${queue} \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria3 \
$jar_executor p_bigd_urm tbgd_turm_stg_v1105_plnta_fixedline_lgpd