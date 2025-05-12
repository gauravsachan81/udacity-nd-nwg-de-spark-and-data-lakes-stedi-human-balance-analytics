CREATE EXTERNAL TABLE `machine_learning_curated`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-gs/step_trainer/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer Trusted to Curated', 
  'CreatedByJobRun'='jr_98b060be2fa7600bda2a43da879e8c7779dddba6aac7b7a7af1cd99a126d8741', 
  'classification'='json')