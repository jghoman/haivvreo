DROP TABLE test_serializer;

CREATE TABLE test_serializer(string1 STRING,
                             int1 INT,
                             tinyint1 TINYINT,
                             smallint1 SMALLINT,
                             bigint1 BIGINT,
                             boolean1 BOOLEAN,
                             float1 FLOAT,
                             double1 DOUBLE,
                             list1 ARRAY<STRING>,
                             map1 MAP<STRING,INT>,
                             struct1 STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>,
                             union1 uniontype<FLOAT, BOOLEAN, STRING>,
                             enum1 STRING,
                             nullableint INT,
                             bytes1 ARRAY<TINYINT>,
                             fixed1 ARRAY<TINYINT>)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY ':' MAP KEYS TERMINATED BY '#' LINES TERMINATED BY '\n'
 STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'test_serializer.csv' INTO TABLE test_serializer;

select * from test_serializer;

DROP TABLE as_avro;

CREATE TABLE as_avro(notused INT)
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.url'='hdfs://localhost:9000/schemas/test_serializer.avsc')
  STORED as INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';

insert overwrite table as_avro select * from test_serializer;

select * from as_avro;

