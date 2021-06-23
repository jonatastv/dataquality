

CREATE TABLE h_bigd_dq_db.dq_volumetria_falhas(                              
banco string,                                                               
tabela string,                                                              
dt_foto string,                                                             
var_nome_campo string,
var_formato_dt_foto string
)
ROW FORMAT SERDE                                                                
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'                                   
STORED AS INPUTFORMAT                                                           
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'                             
OUTPUTFORMAT                                                                    
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'

