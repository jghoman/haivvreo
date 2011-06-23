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
import java.io.{ByteArrayOutputStream, File}
import java.nio.ByteBuffer
import java.util._
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic._
import org.apache.avro.Schema

import java.util.Random

// Write out a bunch of random records that exercise all the Avro types supported
// by haivvreo for testing.  I run this from within IntelliJ, which handles
// all the jars, etc.

// config values
val schemaPath = "./src/test/avro/kitchensink.avsc"
val outputPath = "./target/kitchensink.avro"
val totalRecordsToWrite = 100 * 1000
val checkRecords = false  // Check each record against schema?
val compressFile = true
val compressLevel = 6

// actual script
val kitchensinkSchema = Schema.parse(new File(schemaPath))
val baos = new ByteArrayOutputStream()
val gdw = new GenericDatumWriter[GenericRecord](kitchensinkSchema)

val baseDir = new java.io.File("./target")
if(!baseDir.exists) baseDir.mkdir

val dfw = new DataFileWriter(gdw)
if(compressFile) dfw.setCodec(CodecFactory.deflateCodec(compressLevel))
dfw.create(kitchensinkSchema, new File(outputPath));
val random = new Random()

def generateRecord(i : Int, random : Random) = {
  val r = new GenericData.Record(kitchensinkSchema);
  r.put("string1", "string1value" +"%07d".format(i))
  r.put("string2", "string2value" + i)
  r.put("int1", i)
  r.put("boolean1", if(i % 2 == 0) true else false)
  r.put("long1", i.asInstanceOf[Long])
  r.put("float1", i.asInstanceOf[Float] + 0.1f)
  r.put("double1", i.asInstanceOf[Double] + 0.2)

  val innerRecord = new GenericData.Record(kitchensinkSchema.getField("inner_record1").schema);
  innerRecord.put("int_in_inner_record1", 22 + i)
  innerRecord.put("string_in_inner_record1", "value for string_in_inner_record1");
  r.put("inner_record1", innerRecord)

  val enums = scala.List("ENUM1_VALUES_VALUE1","ENUM1_VALUES_VALUE2", "ENUM1_VALUES_VALUE3")
  r.put("enum1", enums(random.nextInt(3)))

  val list1 = new java.util.ArrayList[String]()
  for(j <- 1 to 10) {
    if(random.nextBoolean) list1.add("zero" + j + i)
    if(random.nextBoolean) list1.add("one" + j + i)
    if(random.nextBoolean) list1.add("two"+ j + i)
  }
  r.put("array1", list1)


  val map1 = new Hashtable[String, String]();
  if(random.nextBoolean) map1.put("human", "earth" + i)
  if(random.nextBoolean) map1.put("timelord", "gallifrey" + i)
  if(random.nextBoolean) map1.put("dalek", "skaro" + i)
  r.put("map1", map1)

  (random.nextInt(3)) match {
    case 0 => r.put("union1", random.nextFloat)
    case 1 => r.put("union1", random.nextBoolean())
    case 2 => r.put("union1", "union1value " + i)
  }

  val bytesArray = Array.fill(4) { random.nextInt(256).asInstanceOf[Byte]}
  val fixed = new GenericData.Fixed(bytesArray)
  r.put("fixed1", fixed)

  // null is easy
  r.put("null1", null)

  r.put("UnionNullInt", if(i%2 == 0) i else null)

  val bytesArray2 = Array.fill(random.nextInt(10)) { random.nextInt(256).asInstanceOf[Byte]}
  val bytebuffer = ByteBuffer.wrap(bytesArray2)
  bytebuffer.rewind
  r.put("bytes1", bytebuffer)

  if(checkRecords)
    println("Is valid = " + GenericData.get().validate(kitchensinkSchema, r))
  r
}
for(i <- 1 to (totalRecordsToWrite)) {
  if(i % 100000 == 0) println("at " + i)
  dfw.append(generateRecord(i, random))
}
dfw.close
println("-30-")