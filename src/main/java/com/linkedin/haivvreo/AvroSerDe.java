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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

import java.util.List;
import java.util.Properties;

/**
 * Read or write Avro data from Hive.
 */
public class AvroSerDe implements SerDe {
  public static final String HAIVVREO_SCHEMA = "haivvreo.schema";
  private ObjectInspector oi;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private Schema schema;
  private AvroDeserializer avroDeserializer = null;
  private AvroSerializer avroSerializer = null;

  private boolean badSchema = false;

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    // Reset member variables so we don't get in a half-constructed state
    schema = null;
    oi = null;
    columnNames  = null;
    columnTypes = null;

    schema =  HaivvreoUtils.determineSchemaOrReturnErrorSchema(properties);
    // Configuration is null in some contexts (e.g. TableDesc#getDeserializer)
    // so we have to be defensive here
    if (configuration != null) {
      configuration.set(HAIVVREO_SCHEMA, schema.toString(false));
    }
    badSchema = schema.equals(SchemaResolutionProblem.SIGNAL_BAD_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
    this.columnNames = aoig.getColumnNames();
    this.columnTypes = aoig.getColumnTypes();
    this.oi = aoig.getObjectInspector();
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return AvroGenericRecordWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    if(badSchema) throw new BadSchemaException();
    return getSerializer().serialize(o, objectInspector, columnNames, columnTypes, schema);
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    if(badSchema) throw new BadSchemaException();
    return getDeserializer().deserialize(columnNames, columnTypes, writable, schema);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }

  private AvroDeserializer getDeserializer() {
    if(avroDeserializer == null) avroDeserializer = new AvroDeserializer();

    return avroDeserializer;
  }

  private AvroSerializer getSerializer() {
    if(avroSerializer == null) avroSerializer = new AvroSerializer();

    return avroSerializer;
  }
}
