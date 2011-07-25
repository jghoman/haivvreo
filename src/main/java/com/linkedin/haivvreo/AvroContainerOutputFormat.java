/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.haivvreo;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * Write to an Avro file from a Hive process.
 */
public class AvroContainerOutputFormat implements HiveOutputFormat<LongWritable, AvroGenericRecordWritable> {

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf,
                                                           Path path,
                                                           Class<? extends Writable> valueClass,
                                                           boolean isCompressed,
                                                           Properties properties,
                                                           Progressable progressable) throws IOException {
    Schema schema;
    try {
      schema = HaivvreoUtils.determineSchemaOrThrowException(properties);
    } catch (HaivvreoException e) {
      throw new IOException(e);
    }
    GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dfw = new DataFileWriter<GenericRecord>(gdw);

    dfw.create(schema, path.getFileSystem(jobConf).create(path));
    return new AvroGenericRecordWriter(dfw);
  }

}
