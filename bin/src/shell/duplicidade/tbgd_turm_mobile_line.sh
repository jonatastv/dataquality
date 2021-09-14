#!/bin/bash

CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2

source "$CONF_GLOBAL_FILE"

data_AAAAMM01_d="$(date +%Y%m)01"

data_AAAAMM01_1M=$(echo $(date -d " -1 month" '+%Y%m15')01 | awk '{print substr($0,1,6) substr($0,9,2)}')


dia_do_mes_d="$(date +%d)"

# se o dia for menor que 10, ler dia 1 do mes anterior
if [ $dia_do_mes_d -lt 10 ] ; then
data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 4g \
--executor-cores 10 \
--num-executors 10 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
$jar_executor p_bigd_urm tbgd_turm_mobile_line "$data_AAAAMM01_d" dt_foto 1


# se o dia for maior que 10, ler dia 1 do mes atual
elif [ $dia_do_mes_d -gt 10 ] ; then

time spark-submit --master yarn  \
--queue ${queue} \
--executor-memory 4g \
--executor-cores 10 \
--num-executors 10 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
$jar_executor p_bigd_urm tbgd_turm_mobile_line "$data_AAAAMM01_1M" dt_foto 1


fi

