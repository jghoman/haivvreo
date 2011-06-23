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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;


import java.io.IOException;

/**
 * RecordReader optimized against Avro GenericRecords that returns to record
 * as the value of the k-v pair, as Hive requires.
 */
public class AvroGenericRecordReader implements RecordReader<NullWritable, AvroGenericRecordWritable>, JobConfigurable {
  final private org.apache.avro.file.FileReader<GenericRecord> reader;
  final private long start;
  final private long stop;
  protected JobConf jobConf;

  public AvroGenericRecordReader(JobConf job, FileSplit split, Reporter reporter) throws IOException {
    this.jobConf = job;
    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();
    this.reader = new DataFileReader<GenericRecord>(new FsInput(split.getPath(), job), gdr);
    this.reader.sync(split.getStart());
    this.start = reader.tell();
    this.stop = split.getStart() + split.getLength();
  }

  @Override
  public boolean next(NullWritable nullWritable, AvroGenericRecordWritable record) throws IOException {
    if(!reader.hasNext() || reader.pastSync(stop)) return false;

    GenericData.Record r = (GenericData.Record)reader.next();
    record.setRecord(r);

    return true;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable();
  }

  @Override
  public long getPos() throws IOException {
    return reader.tell();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return stop == start ? 0.0f
                         : Math.min(1.0f, (getPos() - start) / (float)(stop - start));
  }

  @Override
  public void configure(JobConf jobConf) {
    this.jobConf= jobConf;
  }
}
