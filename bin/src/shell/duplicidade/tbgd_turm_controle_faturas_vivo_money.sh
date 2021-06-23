

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue Qualidade \
--executor-memory 19g \
--executor-cores 5 \
--num-executors 2 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
/home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_urm tbgd_turm_controle_faturas_vivo_money "$data_ontem" dt_foto 1

