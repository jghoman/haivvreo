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


// config values
val schemaPath = "./src/test/avro/tojoin.avsc"
val outputPath = "./target/tojoin.avro"
val totalRecordsToWrite = 2 * 1000 * 1000
val checkRecords = false  // Check each record against schema?
val compressFile = false
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
  //r.put("string2", "string2value" + "%07d".format(i))
  r.put("int1", i)
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