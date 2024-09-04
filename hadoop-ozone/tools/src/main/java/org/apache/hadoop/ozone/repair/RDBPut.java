/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.repair;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.ImmutableListDeserializer;
import org.apache.hadoop.ozone.utils.DBDefinitionFactory;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;


/**
 * Tool to update the highest term-index in transactionInfoTable.
 */
@CommandLine.Command(
    name = "put",
    description = "CLI to put a record to a table.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
@MetaInfServices(SubcommandWithParent.class)
public class RDBPut
    implements Callable<Void>, SubcommandWithParent  {

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.ParentCommand
  private RDBRepair parent;

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Table name")
  private String tableName;

  @CommandLine.Option(names = {"--key", "--k"},
      description = "Key of the record to be added to the DB")
  private String key;

  @CommandLine.Option(names = {"--value", "--v"},
      description = "Value of the record to be added to the DB")
  private String value;

  @CommandLine.Option(names = {"--dnSchema", "--dn-schema", "-d"},
      description = "Datanode DB Schema Version: V1/V2/V3",
      defaultValue = "V3")
  private String dnDBSchemaVersion;

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    String dbPath = parent.getDbPath();
    System.out.println("1. enter call: " + dbPath);

    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(
        dbPath);
    System.out.println("2. desc list:  " + cfDescList);

    String errorMessage = tableName + " is not in a column family in DB for the given path.";

    try (ManagedRocksDB db = ManagedRocksDB.open(dbPath, cfDescList, cfHandleList)) {
      System.out.println("3. in try block");
      ColumnFamilyHandle columnFamilyHandle = RocksDBUtils.getColumnFamilyHandle(tableName, cfHandleList);
      System.out.println("4. columnFamilyHandle:  " + columnFamilyHandle);
      if (columnFamilyHandle == null) {
        throw new IllegalArgumentException(errorMessage);
      }

      DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
      DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(Paths.get(dbPath), new OzoneConfiguration());
      System.out.println("5. dbDefinition:  " + dbDefinition);
      if (dbDefinition == null) {
        throw new IllegalArgumentException(errorMessage);
      }

      final DBColumnFamilyDefinition<?, ?> columnFamilyDefinition = dbDefinition.getColumnFamily(tableName);
      System.out.println("6. columnFamilyDefinition:  " + columnFamilyDefinition);
      if (columnFamilyDefinition == null) {
        throw new IllegalArgumentException(errorMessage);
      }

      Class<?> valueClass = columnFamilyDefinition.getValueType();
      Class<?> keyClass = columnFamilyDefinition.getKeyType();
      System.out.println("7. class of value type:  " + valueClass);

      Object newValue;
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.enable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ImmutableList.class, new ImmutableListDeserializer(DatanodeDetails.class));
        module.addKeySerializer(DatanodeDetails.class, new DatanodeDetailsKeySerializer());
        module.addKeyDeserializer(DatanodeDetails.class, new DatanodeDetailsKeyDeserializer());
        objectMapper.registerModule(module);

        newValue = objectMapper.readValue(value, valueClass);
        System.out.println("After objectMapper");
        System.out.println(newValue);
      } catch (IOException e) {
        System.err.println("IOEX in objectMapper");
        e.printStackTrace();
        throw e;
      }

      System.out.println("8. Converting to byte[]");
      Class<?> valueCodecClass = columnFamilyDefinition.getValueCodec().getClass();
      Method valueCodecClassToPersistedFormat = Stream.of(valueCodecClass.getMethods())
          .filter((m) -> m.getName().equals("toPersistedFormat"))
          .findFirst()
          .get();
      System.out.println(" valueCodecClassToPersistedFormat: " + valueCodecClassToPersistedFormat.getName() + valueCodecClassToPersistedFormat.getReturnType());

      Class<?> keyCodecClass = columnFamilyDefinition.getKeyCodec().getClass();
      Method keyCodecClassToPersistedFormat = Stream.of(keyCodecClass.getMethods())
          .filter((m) -> m.getName().equals("toPersistedFormat"))
          .findFirst()
          .get();
      System.out.println(" mm2: " + keyCodecClassToPersistedFormat.getName() + keyCodecClassToPersistedFormat.getReturnType());

      byte[] valueBytes = (byte[]) valueCodecClassToPersistedFormat.invoke(columnFamilyDefinition.getValueCodec(), valueClass.cast(newValue));
      byte[] keyBytes = (byte[]) keyCodecClassToPersistedFormat.invoke(columnFamilyDefinition.getKeyCodec(), keyClass.cast(key));

      System.out.println("Key Bytes: " + keyBytes);
      System.out.println("Value Bytes: " + valueBytes);

      Object oldValue = RocksDBUtils.getValue(db, columnFamilyHandle, key, columnFamilyDefinition.getValueCodec());
      System.out.println("The original value was \n" + oldValue);


      System.out.println("------------------------");
      //db.get().put(columnFamilyHandle, keyBytes, valueBytes);
      Object newValuee = RocksDBUtils.getValue(db, columnFamilyHandle, key, columnFamilyDefinition.getValueCodec());
      System.out.println("The new value is \n" + newValuee);
      System.out.println("------------------------");

    } catch (RocksDBException exception) {
      System.err.println("Failed to update the RocksDB for the given path: " + dbPath);
      System.err.println(exception);
      throw new IOException("Failed to update RocksDB.", exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }

    return null;
  }


/*
  { \"\/voltest1\": {\r\n  \"metadata\" : { },\r\n  \"objectID\" : -4611686018427000000,\r\n  \"updateID\" : -1,\r\n  \"adminName\" : \"om\",\r\n  \"ownerName\" : \"om\",\r\n  \"volume\" : \"voltest1\",\r\n  \"creationTime\" : 1722579699999,\r\n  \"modificationTime\" : 1722579979329,\r\n  \"quotaInBytes\" : -1,\r\n  \"quotaInNamespace\" : -1,\r\n  \"usedNamespace\" : 0,\r\n  \"acls\" : [ {\r\n    \"type\" : \"USER\",\r\n    \"name\" : \"om\",\r\n    \"aclScope\" : \"ACCESS\"\r\n  }, {\r\n    \"type\" : \"GROUP\",\r\n    \"name\" : \"om\",\r\n    \"aclScope\" : \"ACCESS\"\r\n  } ],\r\n  \"refCount\" : 0\r\n}\r\n }

  { \"\/voltest1\": {  \"metadata\" : { },  \"objectID\" : -4611686018427000000,  \"updateID\" : -1,  \"adminName\" : \"om\",  \"ownerName\" : \"om\",  \"volume\" : \"voltest1\",  \"creationTime\" : 1722579699999,  \"modificationTime\" : 1722579979329,  \"quotaInBytes\" : -1,  \"quotaInNamespace\" : -1,  \"usedNamespace\" : 0,  \"acls\" : [ {    \"type\" : \"USER\",    \"name\" : \"om\",    \"aclScope\" : \"ACCESS\"  }, {    \"type\" : \"GROUP\",    \"name\" : \"om\",    \"aclScope\" : \"ACCESS\"  } ],  \"refCount\" : 0} }

  */

  @Override
  public Class<?> getParentType() {
    return RDBRepair.class;
  }

}

class DatanodeDetailsKeySerializer extends JsonSerializer<DatanodeDetails> {
  @Override
  public void serialize(DatanodeDetails value, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeBinary(value.toProto(ClientVersion.DEFAULT_VERSION.toProtoValue()).toByteArray());
  }
}

class DatanodeDetailsKeyDeserializer extends KeyDeserializer {
  @Override
  public DatanodeDetails deserializeKey(String p, DeserializationContext context) throws IOException {
    return DatanodeDetails.getFromProtoBuf(HddsProtos.DatanodeDetailsProto.parseFrom(p.getBytes()));
  }
}
