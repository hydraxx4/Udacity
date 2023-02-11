CREATE EXTERNAL TABLE IF NOT EXISTS `trungt`.`step_trainer_trusted` (
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` int,
  `timestamp` bigint,
  `birthday` string,
  `sharewithresearchasofdate` bigint,
  `registrationdate` bigint,
  `customername` string,
  `email` string,
  `lastupdatedate` bigint,
  `phone` string,
  `sharewithpublicasofdate` bigint,
  `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://trungt-lake-house/step_trainer/trusted/'
TBLPROPERTIES ('classification' = 'json');