
data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`

data=`date "+%Y%m%d%H%M%S"`

 time spark-submit --master yarn  \
--queue Qualidade \
--executor-memory 20g \
--executor-cores 10 \
--num-executors 10 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidadeNext \
/home/SPDQ/indra/dataquality_2.10-0.1.jar h_bigd_ods_db com_amdocs_insight_dh_shared_mutables_payment_paymentmutable "$data_ontem" p_date 2

