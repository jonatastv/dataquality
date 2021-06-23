data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`


 time spark-submit --master yarn  \
--queue Qualidade \
--executor-memory 4g \
--executor-cores 10 \
--num-executors 5 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade3 \
/home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_db tbgd_brau_meio_cntt_clnt_whlt dt_foto

