data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`
data=`date "+%Y%m%d%H%M%S"`

 time spark-submit --master yarn  \
--queue Qualidade \
--executor-memory 4g \
--executor-cores 10 \
--num-executors 100 \
--class br.com.vivo.dataquality.duplicidade.ColetaDuplicidade \
/home/SPDQ/indra/dataquality_2.10-0.1.jar p_bigd_db tbgdt_atlys_bsv_pymt "$data_ontem" dt_foto 1 
#&> log/tbgdt_atlys_bsv_pymt_"$data"_txt

