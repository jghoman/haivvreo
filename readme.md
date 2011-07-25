Hive + Avro = Haivvreo: Putting Avro into Hive
==============================================

Overview
--------
Haivvreo (pronounced with as many diphthongs as possible. Diphthongs are cool, like bowties.) is a Hive Serde that LinkedIn has developed to process Avro-encoded data in Hive.  Haivvreo's bullet points:

* Infers the schema of the Hive table from the Avro schema.
* Reads all Avro files within a table against a specified schema, taking advantage of Avro's backwards compatibility abilities
* Supports arbitrarily nested schemas.
* Translates all Avro data types into equivalent Hive types.  Most types map exactly, but some Avro types don't exist in Hive and are automatically converted by Haivvreo.
* Understands compressed Avro files.
* Transparently converts the Avro idiom of handling nullable types as Union[T, null] into just T and returns null when appropriate.
* Writes any Hive table to Avro files.
* Has worked reliably against our most convoluted Avro schemas in our ETL process.

License
-------
Haivvreo is Apache 2 licensed.

Requirements
------------
Haivvreo has been built and tested against Hive 0.7 and Avro 1.4.1.  Its build script is built on Maven 3.

Building and deploying
----------------------
Once the jar has been has been built via maven, it and Avro and Avro's transitive dependencies (avro-1.4.1.jar, jackson-core-asl-1.4.2.jar, jackson-mapper-asl-1.4.2.jar) should be made available to Hive either by placing them in the cluster's lib folders or by pointing [HIVE_AUX_JARS_PATH](http://wiki.apache.org/hadoop/Hive/AdminManual/Configuration) to a location with them.

Avro to Hive type conversion
----------------------------
While most Avro types convert directly to equivalent Hive types, there are some which do not exist in Hive and are converted to reasonable equivalents.  Also, Haivvreo special cases unions of null and another type, as described below:
<table>
  <tr>
    <th>Avro type</th><th>Becomes Hive type</th><th>Note</th>
  </tr>
  <tr><td>null</td><td>void</td><td></td></tr>
  <tr><td>boolean</td><td>boolean</td><td></td></tr>
  <tr><td>int</td><td>int</td><td></td></tr>
  <tr><td>long</td><td>bigint</td><td></td></tr>
  <tr><td>float</td><td>float</td><td></td></tr>
  <tr><td>double</td><td>double</td><td></td></tr>
  <tr><td>bytes</td><td>Array[smallint]</td><td>Hive converts these to signed bytes. </td></tr>
  <tr><td>string</td><td>string</td><td></td></tr>
  <tr><td>record</td><td>struct</td><td></td></tr>
  <tr><td>map</td><td>map</td><td></td></tr>
  <tr><td>list</td><td>array</td><td></td></tr>
  <tr><td>union</td><td>union</td><td>Unions of [T, null] transparently convert to nullable T, other types translate directly to Hive's unions of those types.  However, unions were introduced in Hive 7 and are not currently able to be used in where/group-by statements.  They are essentially look-at-only.  I'll file a bug with Hive on this.  Because Haivvreo transparently converts [T,null], to nullable T, this limitation only applies to unions of multiple types or unions not of a single type and null. </td></tr>
  <tr><td>enum</td><td>string</td><td>Hive has no concept of enums</td></tr>
  <tr><td>fixed</td><td>Array[smallint]</td><td>Hive converts the bytes to signed int</td></tr>
</table>

Creating Avro-backed Hive tables
--------------------------------
To create a Haivvreo-backed table, specify the serde as com.linkedin.AvroSerDe, specify the inputformat as com.linkedin.haivvreo.AvroContainerInputFormat, and the outputformat as com.linkedin.haivvreo.AvroContainerOutputFormat.  Also provide a location from which Haivvreo will pull the most current schema for the table. For example:
<pre>CREATE TABLE kst
  PARTITIONED BY (ds string)
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.url'='http://schema_provider/kst.avsc')
  STORED AS INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';
</pre>
In this example we're pulling the source-of-truth reader schema from a webserver.  Other options for providing the schema are described below.
Add the Avro files to the database (or create an external table) using [standard Hive operations](http://wiki.apache.org/hadoop/Hive/LanguageManual/DML).
This table might result in a description as below:
<pre>hive> describe kst;                                                                                                                                                                                                                                                                                 
OK                                                                                                                                                                                                                                                                                         
string1 string  from deserializer                                                                                                                                                                                                                                                     
string2 string  from deserializer                                                                                                                                                                                                                                   
int1    int     from deserializer                                                                                                                                                                                                                                                           
boolean1        boolean from deserializer                                                                                                                                                                                                                                             
long1   bigint  from deserializer                                                                                                                                                                                                                                                    
float1  float   from deserializer                                                                                                                                                                                                                                                        
double1 double  from deserializer                                                                                                                                                                                                                                                          
inner_record1   struct&lt;int_in_inner_record1:int,string_in_inner_record1:string&gt; from deserializer                                                                                                                                                                                         
enum1   string  from deserializer                                                                                                                                                                                                                                                        
array1  array&lt;string&gt;   from deserializer                                                                                                                                                                                                                                            
map1    map&lt;string,string&gt;      from deserializer                                                                                                                                                                                                                                         
union1  uniontype&lt;float,boolean,string&gt; from deserializer                                                                                                                                                                                                                                
fixed1  array&lt;tinyint&gt;  from deserializer                                                                                                                                                                                                                                                
null1   void    from deserializer                                                                                                                                                                                                                                                                 
unionnullint    int     from deserializer                                                                                                                                                                                                                                               
bytes1  array&lt;tinyint&gt;  from deserializer</pre>


At this point, the Avro-backed table can be worked with in Hive like any other table.  Haivvreo cannot yet show comments included in the Avro schema, though a [JIRA has been opened](https://issues.apache.org/jira/browse/HIVE-2171) for this feature.

Writing tables to Avro files
----------------------------
Haivvreo can serialize any Hive table to Avro files. This makes it effectively an any-Hive-type to Avro converter.  In order to write a table to an Avro file, you must first create an appropriate Avro schema. Create as select type statements are not currently supported.  Types translate as detailed in the table above.  For types that do not translate directly, there are a few items to keep in mind:

* **Types that may be null must be defined as a union of that type and Null within Avro.**  A null in a field that is not so defined with result in an exception during the save.  No changes need be made to the Hive schema to support this, as all fields in Hive can be null.
* Avro Bytes type should be defined in Hive as lists of tiny ints.  Haivvreo will convert these to Bytes during the saving process.
* Avro Fixed type should be defined in Hive as lists of tiny ints.  Haivvreo will convert these to Fixed during the saving process.
* Avro Enum type should be defined in Hive as strings, since Hive doesn't have a concept of enums.  Ensure that only valid enum values are present in the table - trying to save a non-defined enum will result in an exception.

**Example**

Consider the following Hive table, which coincidentally covers all types of Hive data types, making it a good example:
<pre>CREATE TABLE test_serializer(string1 STRING,
                             int1 INT,
                             tinyint1 TINYINT,
                             smallint1 SMALLINT,
                             bigint1 BIGINT,
                             boolean1 BOOLEAN,
                             float1 FLOAT,
                             double1 DOUBLE,
                             list1 ARRAY&lt;STRING&gt;,
                             map1 MAP&lt;STRING,INT&gt;,
                             struct1 STRUCT&lt;sint:INT,sboolean:BOOLEAN,sstring:STRING&gt;,
                             union1 uniontype&lt;FLOAT, BOOLEAN, STRING&gt;,
                             enum1 STRING,
                             nullableint INT,
                             bytes1 ARRAY&lt;TINYINT&gt;,
                             fixed1 ARRAY&lt;TINYINT&gt;)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY &#x27;,&#x27; COLLECTION ITEMS TERMINATED BY &#x27;:&#x27; MAP KEYS TERMINATED BY &#x27;#&#x27; LINES TERMINATED BY &#x27;\n&#x27;
 STORED AS TEXTFILE;</pre>
To save this table as an Avro file, create an equivalent Avro schema (the namespace and actual name of the record are not important):
<pre>{
  &quot;namespace&quot;: &quot;com.linkedin.haivvreo&quot;,
  &quot;name&quot;: &quot;test_serializer&quot;,
  &quot;type&quot;: &quot;record&quot;,
  &quot;fields&quot;: [
    { &quot;name&quot;:&quot;string1&quot;, &quot;type&quot;:&quot;string&quot; },
    { &quot;name&quot;:&quot;int1&quot;, &quot;type&quot;:&quot;int&quot; },
    { &quot;name&quot;:&quot;tinyint1&quot;, &quot;type&quot;:&quot;int&quot; },
    { &quot;name&quot;:&quot;smallint1&quot;, &quot;type&quot;:&quot;int&quot; },
    { &quot;name&quot;:&quot;bigint1&quot;, &quot;type&quot;:&quot;long&quot; },
    { &quot;name&quot;:&quot;boolean1&quot;, &quot;type&quot;:&quot;boolean&quot; },
    { &quot;name&quot;:&quot;float1&quot;, &quot;type&quot;:&quot;float&quot; },
    { &quot;name&quot;:&quot;double1&quot;, &quot;type&quot;:&quot;double&quot; },
    { &quot;name&quot;:&quot;list1&quot;, &quot;type&quot;:{&quot;type&quot;:&quot;array&quot;, &quot;items&quot;:&quot;string&quot;} },
    { &quot;name&quot;:&quot;map1&quot;, &quot;type&quot;:{&quot;type&quot;:&quot;map&quot;, &quot;values&quot;:&quot;int&quot;} },
    { &quot;name&quot;:&quot;struct1&quot;, &quot;type&quot;:{&quot;type&quot;:&quot;record&quot;, &quot;name&quot;:&quot;struct1_name&quot;, &quot;fields&quot;: [
          { &quot;name&quot;:&quot;sInt&quot;, &quot;type&quot;:&quot;int&quot; }, { &quot;name&quot;:&quot;sBoolean&quot;, &quot;type&quot;:&quot;boolean&quot; }, { &quot;name&quot;:&quot;sString&quot;, &quot;type&quot;:&quot;string&quot; } ] } },
    { &quot;name&quot;:&quot;union1&quot;, &quot;type&quot;:[&quot;float&quot;, &quot;boolean&quot;, &quot;string&quot;] },
    { &quot;name&quot;:&quot;enum1&quot;, &quot;type&quot;:{&quot;type&quot;:&quot;enum&quot;, &quot;name&quot;:&quot;enum1_values&quot;, &quot;symbols&quot;:[&quot;BLUE&quot;,&quot;RED&quot;, &quot;GREEN&quot;]} },
    { &quot;name&quot;:&quot;nullableint&quot;, &quot;type&quot;:[&quot;int&quot;, &quot;null&quot;] },
    { &quot;name&quot;:&quot;bytes1&quot;, &quot;type&quot;:&quot;bytes&quot; },
    { &quot;name&quot;:&quot;fixed1&quot;, &quot;type&quot;:{&quot;type&quot;:&quot;fixed&quot;, &quot;name&quot;:&quot;threebytes&quot;, &quot;size&quot;:3} }
  ] }</pre>
If the table were backed by a csv  such as:
<table>
<tr><td>why hello there </td><td>42 </td><td>3 </td><td>100 </td><td>1412341 </td><td>true </td><td>42.43 </td><td>85.23423424 </td><td>alpha:beta:gamma </td><td>Earth#42:Control#86:Bob#31 </td><td>17:true:Abe Linkedin </td><td>0:3.141459 </td><td>BLUE </td><td>72 </td><td>0:1:2:3:4:5 </td><td>50:51:53</td></tr>
<tr><td>another record </td><td>98 </td><td>4 </td><td>101 </td><td>9999999 </td><td>false </td><td>99.89 </td><td>0.00000009 </td><td>beta </td><td>Earth#101 </td><td>1134:false:wazzup </td><td>1:true </td><td>RED </td><td>NULL </td><td>6:7:8:9:10 </td><td>54:55:56</td></tr>
<tr><td>third record </td><td>45 </td><td>5 </td><td>102 </td><td>999999999 </td><td>true </td><td>89.99 </td><td>0.00000000000009 </td><td>alpha:gamma </td><td>Earth#237:Bob#723 </td><td>102:false:BNL </td><td>2:Time to go home </td><td>GREEN </td><td>NULL </td><td>11:12:13 </td><td>57:58:59</td></tr>
</table>
one can write it out to Avro with:
<pre>CREATE TABLE as_avro
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.url'='file:///path/to/the/schema/test_serializer.avsc')
  STORED as INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';
insert overwrite table as_avro select * from test_serializer;</pre>
The files that are written by the Hive job are valid Avro files, however, MapReduce doesn't add the standard .avro extension.  If you copy these files out, you'll likely want to rename them with .avro.

Hive is very forgiving about types: it will attempt to store whatever value matches the provided column in the equivalent column position in the new table.  No matching is done on column names, for instance.  Therefore, it is incumbent on the query writer to make sure the the target column types are correct.  If they are not, Avro may accept the type or it may throw an exception, this is dependent on the particular combination of types.

Specifying the Avro schema for a table
--------------------------------------
There are three ways to provide the reader schema for an Avro table, all of which involve parameters to the serde.  As the schema involves, one can update these values by updating the parameters in the table.

**Use schema.url**

Specifies a url to access the schema from. For http schemas, this works for testing and small-scale clusters, but as the schema will be accessed at least once from each task in the job, this can quickly turn the job into a DDOS attack against the URL provider (a web server, for instance).  Use caution when using this parameter for anything other than testing.

The schema can also point to a location on HDFS, for instance: hdfs://your-nn:9000/path/to/avsc/file.  Haivvreo will then read the file from HDFS, which should provide resiliency against many reads at once.  Note that the serde will read this file from every mapper, so it's a good idea to turn the replication of the schema file to a high value to provide good locality for the readers.  The schema file itself should be relatively small, so this does not add a significant amount of overhead to the process.

**Use schema.literal and embed the schema in the create statement**

One can embed the schema directly into the create statement.  This works if the schema doesn't have any single quotes (or they are appropriately escaped), as Hive uses this to define the parameter value.  For instance:
<pre>CREATE TABLE embedded
  COMMENT "just drop the schema right into the HQL"
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.literal'='{
      "namespace": "com.linkedin.haivvreo",
      "name": "some_schema",
      "type": "record",
      "fields": [ { "name":"string1","type":"string"}]
    }')
  STORED AS INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';
</pre>
Note that the value is enclosed in single quotes and just pasted into the create statement.

**Use schema.literal and pass the schema into the script**

Hive can do simple variable substitution and one can pass the schema embedded in a variable to the script.  Note that to do this, the schema must be completely escaped (carriage returns converted to \n, tabs to \t, quotes escaped, etc).  An example:
<pre>set hiveconf:schema;
DROP TABLE example;
CREATE TABLE example
  ROW FORMAT SERDE
  'com.linkedin.haivvreo.AvroSerDe'
  WITH SERDEPROPERTIES (
    'schema.literal'='${hiveconf:schema}')

  STORED AS INPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerInputFormat'
  OUTPUTFORMAT
  'com.linkedin.haivvreo.AvroContainerOutputFormat';
</pre>
To execute this script file, assuming $SCHEMA has been defined to be the escaped schema value:
<pre>hive -hiveconf schema="${SCHEMA}" -f your_script_file.sql</pre>
Note that $SCHEMA is interpolated into the quotes to correctly handle spaces within the schema.

**Use none to ignore either schema.literal or schema.url**

Hive does not provide an easy way to unset or remove a property. If you wish to switch from using url or schema to the other, set the to-be-ignored value to *none* and Haivvreo will treat it as if it were not set.

If something goes wrong
-----------------------
Hive tends to swallow exceptions from Haivvreo that occur before job submission. To force Hive to be more verbose, it can be started with **hive -hiveconf hive.root.logger=INFO,console**, which will spit orders of magnitude more information to the console and will likely include any information Haivvreo is trying to get you about what went wrong.  If Haivvreo encounters an error during MapReduce, the stack trace will be provided in the failed task log, which can be examined from the JobTracker's web interface.  Haivvreo only emits HaivvreoException; look for these.  Please include these in any bug reports.  The most common is expected to be exceptions while attempting to serializing an incompatible type from what Avro is expecting.

FAQ
---
* Why do I get **error-error-error-error-error-error-error** and a message to check schema.literal and schema.url when describing a table or ruinning a query against a table?

> Haivvreo returns this message when it has trouble finding or parsing the schema provided by either the schema.literal or schema.url value.  It is unable to be more specific because Hive expects all calls to the serde config methods to be successful, meaning we are unable to return an actual exception.   By signaling an error via this message, the table is left in a good state and the incorrect value can be corrected with a call to *alter table T set serdeproperties*.

* Why do I get a **java.io.IOException: com.linkedin.haivvreo.HaivvreoException: Neither schema.literal nor schema.url specified, can't determine table schema** when pruning by a partition that doesn't exist?

> Hive creates a temporary empty file for non-existent partitions in order that queries referencing them succeed (returning a count of zero rows).  However, when doing so, it doesn't pass the correct information to the RecordWriter, leaving Haivvreo unable to construct one.  This problem has been corrected in [Hive 0.8](https://issues.apache.org/jira/browse/HIVE-2260) .  With previous versions of Hive, either be sure to only filter on existing partitions or apply HIVE-2260.

 What's next
-----------
We're currently testing Haivvreo in our production ETL process and have found it reliable and flexible.  We'll be working on improving its performance (there are several opportunities for improvement both in Hive and Haivvreo itself).  The next feature we'll add is schema induction when writing from Hive tables so that it is not necessary to provide an equivalent Avro schema ahead of time.

Please kick the tires and file bugs.

