
data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`
database='p_bigd_db'
table='tbgd_tedw_vivo_money_base'

time spark-submit --master yarn  \
--queue Qualidade \
--class br.com.vivo.dataquality.duplicidade.JuntaTabela \
 /home/SPDQ/indra/dataquality_2.10-0.1.jar ${database} ${table} "$data_ontem" dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_teste
