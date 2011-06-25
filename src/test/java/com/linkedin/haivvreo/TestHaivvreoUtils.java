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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static com.linkedin.haivvreo.HaivvreoUtils.*;
import static org.junit.Assert.fail;

public class TestHaivvreoUtils {
  private final String NULLABLE_UNION = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"nullTest\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"mayBeNull\", \"type\":[\"string\", \"null\"]}\n" +
      "  ]\n" +
      "}";
  // Same union, order reveresed
  private final String NULLABLE_UNION2 = "{\n" +
    "  \"type\": \"record\", \n" +
    "  \"name\": \"nullTest\",\n" +
    "  \"fields\" : [\n" +
    "    {\"name\":\"mayBeNull\", \"type\":[\"null\", \"string\"]}\n" +
    "  ]\n" +
    "}";

  private void testField(String schemaString, String fieldName, boolean shouldBeNullable) {
    Schema s = Schema.parse(schemaString);
    assertEquals(shouldBeNullable, isNullableType(s.getField(fieldName).schema()));
  }

  @Test
  public void isNullableTypeAcceptsNullableUnions() {
    testField(NULLABLE_UNION, "mayBeNull", true);
    testField(NULLABLE_UNION2, "mayBeNull", true);
  }

  @Test
  public void isNullableTypeIdentifiesUnionsOfMoreThanTwoTypes() {
    String schemaString = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"shouldNotPass\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"mayBeNull\", \"type\":[\"string\", \"int\", \"null\"]}\n" +
      "  ]\n" +
      "}";
    testField(schemaString, "mayBeNull", false);
  }

  @Test
  public void isNullableTypeIdentifiesUnionsWithoutNulls() {
    String s = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"unionButNoNull\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"a\", \"type\":[\"int\", \"string\"]}\n" +
      "  ]\n" +
      "}";
    testField(s, "a", false);
  }

  @Test
  public void isNullableTypeIdentifiesNonUnionTypes() {
    String schemaString = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"nullTest2\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"justAnInt\", \"type\":\"int\"}\n" +
      "  ]\n" +
      "}";
    testField(schemaString, "justAnInt", false);
  }

  @Test
  public void getTypeFromNullableTypePositiveCase() {
    Schema s = Schema.parse(NULLABLE_UNION);
    Schema typeFromNullableType = getOtherTypeFromNullableType(s.getField("mayBeNull").schema());
    assertEquals(Schema.Type.STRING, typeFromNullableType.getType());

    s = Schema.parse(NULLABLE_UNION2);
    typeFromNullableType = getOtherTypeFromNullableType(s.getField("mayBeNull").schema());
    assertEquals(Schema.Type.STRING, typeFromNullableType.getType());
  }

  @Test(expected=HaivvreoException.class)
  public void determineSchemaThrowsExceptionIfNoSchema() throws IOException, HaivvreoException {
    Properties prop = new Properties();
    HaivvreoUtils.determineSchema(prop);
  }

  @Test
  public void determineSchemaFindsLiterals() throws Exception {
    String schema = TestAvroObjectInspectorGenerator.RECORD_SCHEMA;
    Properties props = new Properties();
    props.put(HaivvreoUtils.SCHEMA_LITERAL, schema);
    Schema expected = Schema.parse(schema);
    assertEquals(expected, HaivvreoUtils.determineSchema(props));
  }

  @Test
  public void detemineSchemaTriesToOpenUrl() throws HaivvreoException, IOException {
    Properties props = new Properties();
    props.put(HaivvreoUtils.SCHEMA_URL, "not:///a.real.url");

    try {
      HaivvreoUtils.determineSchema(props);
      fail("Should have tried to open that URL");
    } catch(MalformedURLException e) {
      assertEquals("unknown protocol: not", e.getMessage());
    }
  }

  @Test
  public void determineSchemaCanReadSchemaFromHDFS() throws IOException, HaivvreoException {
    // TODO: Make this an integration test, mock out hdfs for the actual unit test.
    String schemaString = TestAvroObjectInspectorGenerator.RECORD_SCHEMA;
    MiniDFSCluster miniDfs = null;
    try {
      // MiniDFSCluster litters files and folders all over the place.
      System.setProperty("test.build.data", "target/test-intermediate-stuff-data/");
      miniDfs = new MiniDFSCluster(new Configuration(), 1, true, null);

      miniDfs.getFileSystem().mkdirs(new Path("/path/to/schema"));
      FSDataOutputStream out = miniDfs.getFileSystem().create(new Path("/path/to/schema/schema.avsc"));
      out.writeBytes(schemaString);
      out.close();
      String onHDFS = miniDfs.getFileSystem().getUri() + "/path/to/schema/schema.avsc";

      Schema schemaFromHDFS = HaivvreoUtils.getSchemaFromHDFS(onHDFS, miniDfs.getFileSystem().getConf());
      Schema expectedSchema = Schema.parse(schemaString);
      assertEquals(expectedSchema, schemaFromHDFS);
    } finally {
      if(miniDfs != null) miniDfs.shutdown();
    }
  }
}
