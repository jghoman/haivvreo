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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Read or write Avro data from Hive.
 */
public class AvroSerDe implements SerDe {
  private static final Log LOG = LogFactory.getLog(AvroSerDe.class);

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
    if(schema != null)
      LOG.info("Resetting already initialized AvroSerDe");

    schema = null;
    oi = null;
    columnNames  = null;
    columnTypes = null;

    properties = determineCorrectProperties(configuration, properties);

    schema =  HaivvreoUtils.determineSchemaOrReturnErrorSchema(properties);
    if(configuration == null) {
      LOG.info("Configuration null, not inserting schema");
    } else {
      // force output files to have a .avro extension
      configuration.set("hive.output.file.extension", ".avro");
      configuration.set(HAIVVREO_SCHEMA, schema.toString(false));
    }

    badSchema = schema.equals(SchemaResolutionProblem.SIGNAL_BAD_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
    this.columnNames = aoig.getColumnNames();
    this.columnTypes = aoig.getColumnTypes();
    this.oi = aoig.getObjectInspector();
  }

  // Hive passes different properties in at different times.  If we're in a MR job,
  // we'll get properties for the partition rather than the table, which will give
  // us old values for the schema (if it's evolved).  Therefore, in an MR job
  // we need to extract the table properties.
  // Also, in join queries, multiple properties will be included, so we need
  // to extract out the one appropriate to the table we're serde'ing.
  private Properties determineCorrectProperties(Configuration configuration, Properties properties) {
    if((configuration instanceof JobConf) && HaivvreoUtils.insideMRJob((JobConf) configuration)) {
      LOG.info("In MR job, extracting table-level properties");
      MapredWork mapRedWork = Utilities.getMapRedWork(configuration);
      LinkedHashMap<String,PartitionDesc> a = mapRedWork.getAliasToPartnInfo();
      if(a.size() == 1) {
        LOG.info("Only one PartitionDesc found.  Returning that Properties");
        PartitionDesc p = a.values().iterator().next();
        TableDesc tableDesc = p.getTableDesc();
        return tableDesc.getProperties();
      } else {
        String tableName = properties.getProperty("name");
        LOG.info("Multiple PartitionDescs.  Return properties for " + tableName);

        for (Map.Entry<String, PartitionDesc> partitionDescs : a.entrySet()) {
          Properties p = partitionDescs.getValue().getTableDesc().getProperties();
          if(p.get("name").equals(tableName)) {
            // We've found the matching table partition
            LOG.info("Matched table name against " + partitionDescs.getKey() + ", return its properties");
            return p;
          }
        }
        // Didn't find anything in partitions to match on.  WARN, at least.
        LOG.warn("Couldn't find any matching properties for table: " +
                tableName + ". Returning original properties");
      }

    }
    return properties;
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

  @Override
  public SerDeStats getSerDeStats() {
    // No support for statistics. That seems to be a popular answer.
    return null;
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
