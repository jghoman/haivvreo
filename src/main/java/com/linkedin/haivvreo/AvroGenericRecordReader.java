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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;


import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

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
    Schema latest;
    try {
      latest = getSchema(job);
    } catch (HaivvreoException e) {
      throw new IOException(e);
    }
    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();
    if(latest != null) gdr.setExpected(latest);
    this.reader = new DataFileReader<GenericRecord>(new FsInput(split.getPath(), job), gdr);
    this.reader.sync(split.getStart());
    this.start = reader.tell();
    this.stop = split.getStart() + split.getLength();
  }

  /**
   * Attempt to retrieve the reader schema.  Haivvreo has a couple opportunities
   * to provide this, depending on whether or not we're just selecting data
   * or running with a MR job.
   * @param job
   * @return  Reader schema for the Avro object, or null if it has not been provided.
   * @throws HaivvreoException
   */
  private Schema getSchema(JobConf job) throws HaivvreoException, IOException {
    // In "select * from table" situations (non-MR), Haivvreo can add things to the job
    String s = job.get(AvroSerDe.HAIVVREO_SCHEMA);
    if(s != null) return Schema.parse(s);

    // Inside of a MR job, we can pull out the actual properties
    if(Utilities.getHiveJobID(job) != null) {
      MapredWork mapRedWork = Utilities.getMapRedWork(job);
      LinkedHashMap<String,PartitionDesc> aliasToPartnInfo = mapRedWork.getAliasToPartnInfo();
      // Assume that one of the partitions will point back to the table itself.
      // I can't find anything in the Hive code that contradicts this...
      for (PartitionDesc pd : aliasToPartnInfo.values()) {
        Properties props = pd.getProperties();
        if(props.containsKey(HaivvreoUtils.SCHEMA_LITERAL) || props.containsKey(HaivvreoUtils.SCHEMA_URL)) {
          Schema schema = HaivvreoUtils.determineSchemaOrThrowException(props);
          // while we're here, let's stash the schema in the jobconf for anyone coming after us
          job.set(AvroSerDe.HAIVVREO_SCHEMA, schema.toString(false));
          return schema;
        }
      }
    }
    // No more places to get the schema from. Give up.  May have to re-encode later.
    return null;
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
