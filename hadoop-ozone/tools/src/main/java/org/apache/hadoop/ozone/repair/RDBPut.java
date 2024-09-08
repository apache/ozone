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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
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
import org.apache.hadoop.ozone.debug.DBDefinitionFactory;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;


/**
 * Tool to add or update a record in any table in a DB.
 */
@CommandLine.Command(
    name = "put",
    description = "CLI to put a record to a table. Only the following om.db tables are currently supported: " +
        "volumeTable, bucketTable, keyTable, openKeyTable, fileTable, openFileTable, " +
        "directoryTable, deletedDirectoryTable, s3SecretTable.",
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
    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(
        dbPath);
    String errorMessage = tableName + " is not in a column family in DB for the given path.";

    try (ManagedRocksDB db = ManagedRocksDB.open(dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle columnFamilyHandle = RocksDBUtils.getColumnFamilyHandle(tableName, cfHandleList);
      if (columnFamilyHandle == null) {
        throw new IllegalArgumentException(errorMessage);
      }
      DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
      DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(Paths.get(dbPath), new OzoneConfiguration());
      if (dbDefinition == null) {
        throw new IllegalArgumentException(errorMessage);
      }
      final DBColumnFamilyDefinition<?, ?> columnFamilyDefinition = dbDefinition.getColumnFamily(tableName);
      if (columnFamilyDefinition == null) {
        throw new IllegalArgumentException(errorMessage);
      }
      Class<?> valueClass = columnFamilyDefinition.getValueType();
      Class<?> keyClass = columnFamilyDefinition.getKeyType();

      Object newValue;
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
      objectMapper.enable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
      SimpleModule module = new SimpleModule();
      module.addDeserializer(ImmutableList.class, new ImmutableListDeserializer(DatanodeDetails.class));
      module.addKeySerializer(DatanodeDetails.class, new DatanodeDetailsKeySerializer());
      module.addKeyDeserializer(DatanodeDetails.class, new DatanodeDetailsKeyDeserializer());
      objectMapper.registerModule(module);
      try {
        newValue = objectMapper.readValue(value, valueClass);
      } catch (IOException e) {
        e.printStackTrace();
        throw e;
      }

      Class<?> valueCodecClass = columnFamilyDefinition.getValueCodec().getClass();
      Method valueCodecClassToPersistedFormat = Stream.of(valueCodecClass.getMethods())
          .filter((m) -> m.getName().equals("toPersistedFormat"))
          .findFirst()
          .get();
      Class<?> keyCodecClass = columnFamilyDefinition.getKeyCodec().getClass();
      Method keyCodecClassToPersistedFormat = Stream.of(keyCodecClass.getMethods())
          .filter((m) -> m.getName().equals("toPersistedFormat"))
          .findFirst()
          .get();

      byte[] valueBytes = (byte[]) valueCodecClassToPersistedFormat.invoke(columnFamilyDefinition.getValueCodec(),
          valueClass.cast(newValue));
      byte[] keyBytes = (byte[]) keyCodecClassToPersistedFormat.invoke(columnFamilyDefinition.getKeyCodec(),
          keyClass.cast(objectMapper.readValue(key, keyClass)));

      db.get().put(columnFamilyHandle, keyBytes, valueBytes);
      System.out.println("Record put in db successfully.");

    } catch (RocksDBException exception) {
      System.err.println("Failed to update the RocksDB for the given path: " + dbPath);
      System.err.println(exception);
      throw new IOException("Failed to update RocksDB.", exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }

    return null;
  }

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
    return DatanodeDetails.getFromProtoBuf(HddsProtos.DatanodeDetailsProto
        .parseFrom(p.getBytes(StandardCharsets.UTF_8)));
  }
}
