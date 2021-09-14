
CONF_GLOBAL_FILE="/home/SPDQ/indra/config.global.conf"
export SPARK_MAJOR_VERSION=2
source "$CONF_GLOBAL_FILE"

data_ontem=`date -d "yesterday 13:00" '+%Y%m%d'`
database='p_bigd_urm'
table='tbgd_turm_stg_v1106_mtrz_spdy_fixedline_lgpd'

time spark-submit --master yarn  \
--queue Qualidade \
--class br.com.vivo.dataquality.duplicidade.JuntaTabela2 \
$jar_executor ${database} ${table} dq_duplicados_medidas_aux_01_coletaDuplicidade_${database}_${table}_t