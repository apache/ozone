/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Parser for scm.db, om.db or container db file.
 */
@CommandLine.Command(
    name = "scan",
    description = "Parse specified metadataTable"
)
@MetaInfServices(SubcommandWithParent.class)
public class DBScanner implements Callable<Void>, SubcommandWithParent {

  public static final Logger LOG = LoggerFactory.getLogger(DBScanner.class);
  private static final String SCHEMA_V3 = "V3";

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Table name")
  private String tableName;

  @CommandLine.Option(names = {"--with-keys"},
      description = "Print a JSON object of key->value pairs (default)"
          + " instead of a JSON array of only values.",
      defaultValue = "true")
  private boolean withKey;

  @CommandLine.Option(names = {"--length", "--limit", "-l"},
      description = "Maximum number of items to list.",
      defaultValue = "-1")
  private long limit;

  @CommandLine.Option(names = {"--out", "-o"},
      description = "File to dump table scan data")
  private String fileName;

  @CommandLine.Option(names = {"--startkey", "--sk", "-s"},
      description = "Key from which to iterate the DB")
  private String startKey;

  @CommandLine.Option(names = {"--dnSchema", "--dn-schema", "-d"},
      description = "Datanode DB Schema Version: V1/V2/V3",
      defaultValue = "V2")
  private String dnDBSchemaVersion;

  @CommandLine.Option(names = {"--container-id", "--cid"},
      description = "Container ID. Applicable if datanode DB Schema is V3",
      defaultValue = "-1")
  private long containerId;

  @CommandLine.Option(names = { "--show-count", "--count" },
      description = "Get estimated key count for the given DB column family",
      defaultValue = "false",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private boolean showCount;

  @Override
  public Void call() throws Exception {

    List<ColumnFamilyDescriptor> cfDescList =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
    final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();

    try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(
        parent.getDbPath(), cfDescList, cfHandleList)) {
      printTable(cfHandleList, db, parent.getDbPath());
    }

    return null;
  }

  private PrintWriter err() {
    return spec.commandLine().getErr();
  }

  private PrintWriter out() {
    return spec.commandLine().getOut();
  }

  public byte[] getValueObject(
      DBColumnFamilyDefinition dbColumnFamilyDefinition) {
    Class<?> keyType = dbColumnFamilyDefinition.getKeyType();
    if (keyType.equals(String.class)) {
      return startKey.getBytes(UTF_8);
    } else if (keyType.equals(ContainerID.class)) {
      return new ContainerID(Long.parseLong(startKey)).getBytes();
    } else if (keyType.equals(Long.class)) {
      return Longs.toByteArray(Long.parseLong(startKey));
    } else if (keyType.equals(PipelineID.class)) {
      return PipelineID.valueOf(UUID.fromString(startKey)).getProtobuf()
          .toByteArray();
    } else {
      throw new IllegalArgumentException(
          "StartKey is not supported for this table.");
    }
  }

  private void displayTable(ManagedRocksIterator iterator,
      DBColumnFamilyDefinition dbColumnFamilyDefinition) throws IOException {

    if (fileName != null) {
      try (PrintWriter out = new PrintWriter(fileName, UTF_8.name())) {
        displayTable(iterator, dbColumnFamilyDefinition, out);
      }
    } else {
      displayTable(iterator, dbColumnFamilyDefinition, out());
    }
  }

  private void displayTable(ManagedRocksIterator iterator,
      DBColumnFamilyDefinition dbColumnFamilyDefinition, PrintWriter out)
      throws IOException {

    if (startKey != null) {
      iterator.get().seek(getValueObject(dbColumnFamilyDefinition));
    }

    if (withKey) {
      // Start JSON object (map)
      out.print("{ ");
    } else {
      // Start JSON array
      out.print("[ ");
    }

    boolean schemaV3 = dnDBSchemaVersion != null &&
        dnDBSchemaVersion.equalsIgnoreCase(SCHEMA_V3);

    // Count number of keys printed so far
    long count = 0;
    while (withinLimit(count) && iterator.get().isValid()) {
      StringBuilder sb = new StringBuilder();
      if (withKey) {
        Object key = dbColumnFamilyDefinition.getKeyCodec()
            .fromPersistedFormat(iterator.get().key());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        if (schemaV3) {
          int index =
              DatanodeSchemaThreeDBDefinition.getContainerKeyPrefixLength();
          String cid = key.toString().substring(0, index);
          String blockId = key.toString().substring(index);
          sb.append(gson.toJson(Longs.fromByteArray(
              FixedLengthStringUtils.string2Bytes(cid)) + ": " + blockId));
        } else {
          sb.append(gson.toJson(key));
        }
        sb.append(": ");
      }

      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      Object o = dbColumnFamilyDefinition.getValueCodec()
          .fromPersistedFormat(iterator.get().value());
      sb.append(gson.toJson(o));

      iterator.get().next();
      ++count;
      if (withinLimit(count) && iterator.get().isValid()) {
        // If this is not the last entry, append comma
        sb.append(", ");
      }

      out.print(sb);
    }

    if (withKey) {
      // End JSON object
      out.println(" }");
    } else {
      // End JSON array
      out.println(" ]");
    }
  }

  private boolean withinLimit(long i) {
    return limit == -1L || i < limit;
  }

  private ColumnFamilyHandle getColumnFamilyHandle(
      byte[] name, List<ColumnFamilyHandle> columnFamilyHandles) {
    return columnFamilyHandles
        .stream()
        .filter(
            handle -> {
              try {
                return Arrays.equals(handle.getName(), name);
              } catch (Exception ex) {
                throw new RuntimeException(ex);
              }
            })
        .findAny()
        .orElse(null);
  }

  /**
   * Main table printing logic.
   * User-provided args are not in the arg list. Those are instance variables
   * parsed by picocli.
   */
  private void printTable(List<ColumnFamilyHandle> columnFamilyHandleList,
                          ManagedRocksDB rocksDB, String dbPath)
      throws IOException, RocksDBException {

    if (limit < 1 && limit != -1) {
      throw new IllegalArgumentException(
          "List length should be a positive number. Only allowed negative" +
              " number is -1 which is to dump entire table");
    }
    dbPath = removeTrailingSlashIfNeeded(dbPath);
    DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
    DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(
        Paths.get(dbPath), new OzoneConfiguration());
    if (dbDefinition == null) {
      err().println("Error: Incorrect DB Path");
      return;
    }

    Map<String, DBColumnFamilyDefinition> columnFamilyMap = new HashMap<>();
    for (DBColumnFamilyDefinition cfDef : dbDefinition.getColumnFamilies()) {
      LOG.info("Found table: {}", cfDef.getTableName());
      columnFamilyMap.put(cfDef.getTableName(), cfDef);
    }
    if (!columnFamilyMap.containsKey(tableName)) {
      err().print("Error: Table with name '" + tableName + "' not found");
      return;
    }

    DBColumnFamilyDefinition columnFamilyDefinition =
        columnFamilyMap.get(tableName);
    ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
        columnFamilyDefinition.getTableName().getBytes(UTF_8),
        columnFamilyHandleList);
    if (columnFamilyHandle == null) {
      throw new IllegalStateException("columnFamilyHandle is null");
    }

    if (showCount) {
      long keyCount = rocksDB.get()
          .getLongProperty(columnFamilyHandle, RocksDatabase.ESTIMATE_NUM_KEYS);
      out().println(keyCount);
      return;
    }

    ManagedRocksIterator iterator = null;
    try {
      if (containerId > 0L && dnDBSchemaVersion != null
          && dnDBSchemaVersion.equalsIgnoreCase(SCHEMA_V3)) {
        // Handle SchemaV3 DN DB
        ManagedReadOptions readOptions = new ManagedReadOptions();
        readOptions.setIterateUpperBound(new ManagedSlice(
            FixedLengthStringUtils.string2Bytes(
                DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix(
                    containerId + 1L))));
        iterator = new ManagedRocksIterator(
            rocksDB.get().newIterator(columnFamilyHandle, readOptions));
        iterator.get().seek(FixedLengthStringUtils.string2Bytes(
            DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix(
                containerId)));
      } else {
        iterator = new ManagedRocksIterator(
            rocksDB.get().newIterator(columnFamilyHandle));
        iterator.get().seekToFirst();
      }

      displayTable(iterator, columnFamilyDefinition);
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)) {
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }
}
