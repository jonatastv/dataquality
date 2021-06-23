#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

data=`date "+%Y%m%d%H%M%S"`

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 27g \
--driver-memory 3g \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
$jar_executor p_bigd_urm tbgd_turm_bill "$data_ontem" dt_foto 1 

#&> log/tbgd_turm_bill_"$data"_txt



