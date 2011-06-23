DROP TABLE kst;

CREATE TABLE kst 
  COMMENT 'How many datatypes can we stuff into one table'
  PARTITIONED BY (ds String)
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.url'='hdfs://localhost:9000/schemas/kitchensink.avsc')
  STORED AS INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';

describe kst;


