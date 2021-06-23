

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

time spark-submit --master yarn  \
--queue Qualidade \
--executor-memory 8g \
--executor-cores 10 \
--num-executors 2 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
/home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_db tbgd_turm_customer "$data_ontem" dt_foto 1


