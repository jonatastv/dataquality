
data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`
database='h_bigd_ods_db'
table='com_amdocs_insight_dh_shared_mutables_payment_paymentmutable'

time spark-submit --master yarn  \
--queue Qualidade \
--class br.com.vivo.dataquality.duplicidade.JuntaTabela \
 /home/SPDQ/indra/dataquality_2.10-0.1.jar ${database} ${table} "$data_ontem" dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste
