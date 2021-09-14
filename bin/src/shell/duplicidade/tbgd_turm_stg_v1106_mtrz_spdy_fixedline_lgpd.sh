#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue ${queue} \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade3 \
$jar_executor p_bigd_urm tbgd_turm_stg_v1106_mtrz_spdy_fixedline_lgpd cpf_cnpj