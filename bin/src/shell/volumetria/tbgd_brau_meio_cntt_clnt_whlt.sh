#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

source "$CONF_GLOBAL_FILE"

data_hoje=`date '+%Y%m%d'`

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 40g \
--executor-cores 10 \
--num-executors 3 \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetria3 \
$jar_executor p_bigd_db tbgd_brau_meio_cntt_clnt_whlt "$data_hoje" dt_foto 2
