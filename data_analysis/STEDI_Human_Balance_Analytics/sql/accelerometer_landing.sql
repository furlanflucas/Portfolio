CREATE EXTERNAL TABLE `stedi.accelerometer_landing_folder` (
    `user` STRING COMMENT 'from deserializer',
    `timestamp` BIGINT COMMENT 'from deserializer',
    `x` DOUBLE COMMENT 'from deserializer',
    `y` DOUBLE COMMENT 'from deserializer',
    `z` DOUBLE COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'paths'='timestamp,user,x,y,z'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://accelerometer--bucket/accelerometer_landing_folder/'
TBLPROPERTIES (
    'CrawlerSchemaDeserializerVersion'='1.0',
    'CrawlerSchemaSerializerVersion'='1.0',
    'UPDATED_BY_CRAWLER'='crawler_table_creation',
    'averageRecordSize'='761',
    'classification'='json',
    'compressionType'='none',
    'objectCount'='9',
    'recordCount'='9007',
    'sizeKey'='6871328',
    'typeOfData'='file'
);