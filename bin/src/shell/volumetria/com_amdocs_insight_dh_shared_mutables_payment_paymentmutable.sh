#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`


source "$CONF_GLOBAL_FILE"


#log_file="com_amdocs_insight_dh_shared_mutables_payment_paymentmutable.txt"

#echo '' >> "${dir_log_volumetria}/${log_file}"

 time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 40g \
--executor-cores 10 \
--num-executors 3 \
--class br.com.vivo.dataquality.volumetria.ColetaVolumetriaNext \
$jar_executor h_bigd_ods_db com_amdocs_insight_dh_shared_mutables_payment_paymentmutable "$data_ontem" p_date 2

