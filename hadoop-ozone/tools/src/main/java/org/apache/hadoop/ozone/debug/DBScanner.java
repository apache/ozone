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
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import picocli.CommandLine;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Parser for scm.db, om.db or container db file.
 */
@CommandLine.Command(
        name = "scan",
        description = "Parse specified metadataTable"
)
@MetaInfServices(SubcommandWithParent.class)
public class DBScanner implements Callable<Void>, SubcommandWithParent {

  @CommandLine.Option(names = {"--column_family"},
      required = true,
      description = "Table name")
  private String tableName;

  @CommandLine.Option(names = {"--with-keys"},
      description = "List Key -> Value instead of just Value.",
      defaultValue = "false",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private static boolean withKey;

  @CommandLine.Option(names = {"--length", "-l"},
          description = "Maximum number of items to list. " +
              "If -1 dumps the entire table data")
  private static int limit = 100;

  @CommandLine.Option(names = {"--out", "-o"},
      description = "File to dump table scan data")
  private static String fileName;

  @CommandLine.Option(names = {"--dnSchema", "-d"},
      description = "Datanode DB Schema Version : V1/V2/V3",
      defaultValue = "V2")
  private static String dnDBSchemaVersion;

  @CommandLine.Option(names = {"--container-id", "-cid"},
      description = "Container ID when datanode DB Schema is V3",
      defaultValue = "-1")
  private static long containerId;

  @CommandLine.ParentCommand
  private RDBParser parent;

  private HashMap<String, DBColumnFamilyDefinition> columnFamilyMap;

  private List<Object> scannedObjects;

  private static List<Object> displayTable(ManagedRocksIterator iterator,
      DBColumnFamilyDefinition dbColumnFamilyDefinition) throws IOException {
    List<Object> outputs = new ArrayList<>();

    Writer fileWriter = null;
    PrintWriter printWriter = null;
    try {
      if (fileName != null) {
        fileWriter = new OutputStreamWriter(
            new FileOutputStream(fileName), StandardCharsets.UTF_8);
        printWriter = new PrintWriter(fileWriter);
      }

      boolean schemaV3 = dnDBSchemaVersion != null &&
          dnDBSchemaVersion.equals("V3");
      while (iterator.get().isValid()) {
        StringBuilder result = new StringBuilder();
        if (withKey) {
          Object key = dbColumnFamilyDefinition.getKeyCodec()
              .fromPersistedFormat(iterator.get().key());
          Gson gson = new GsonBuilder().setPrettyPrinting().create();
          if (schemaV3) {
            int index =
                DatanodeSchemaThreeDBDefinition.getContainerKeyPrefixLength();
            String cid = key.toString().substring(0, index);
            String blockId = key.toString().substring(index);
            result.append(gson.toJson(Longs.fromByteArray(
                FixedLengthStringUtils.string2Bytes(cid)) + ": " + blockId));
          } else {
            result.append(gson.toJson(key));
          }
          result.append(" -> ");
        }
        Object o = dbColumnFamilyDefinition.getValueCodec()
            .fromPersistedFormat(iterator.get().value());
        outputs.add(o);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        result.append(gson.toJson(o));
        if (fileName != null) {
          printWriter.println(result);
        } else {
          System.out.println(result.toString());
        }
        limit--;
        iterator.get().next();
        if (limit == 0) {
          break;
        }
      }
    } finally {
      if (printWriter != null) {
        printWriter.close();
      }
      if (fileWriter != null) {
        fileWriter.close();
      }
    }
    return outputs;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public RDBParser getParent() {
    return parent;
  }

  public void setParent(RDBParser parent) {
    this.parent = parent;
  }

  public static void setLimit(int limit) {
    DBScanner.limit = limit;
  }

  public List<Object> getScannedObjects() {
    return scannedObjects;
  }

  public static void setFileName(String name) {
    DBScanner.fileName = name;
  }

  public static void setContainerId(long id) {
    DBScanner.containerId = id;
  }

  public static void setDnDBSchemaVersion(String version) {
    DBScanner.dnDBSchemaVersion = version;
  }

  public static void setWithKey(boolean withKey) {
    DBScanner.withKey = withKey;
  }

  private static ColumnFamilyHandle getColumnFamilyHandle(
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

  private void constructColumnFamilyMap(DBDefinition dbDefinition) {
    if (dbDefinition == null) {
      System.out.println("Incorrect Db Path");
      return;
    }
    this.columnFamilyMap = new HashMap<>();
    DBColumnFamilyDefinition[] columnFamilyDefinitions = dbDefinition
            .getColumnFamilies();
    for (DBColumnFamilyDefinition definition:columnFamilyDefinitions) {
      System.out.println("Added definition for table:" +
          definition.getTableName());
      this.columnFamilyMap.put(definition.getTableName(), definition);
    }
  }

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyDescriptor> cfs =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());

    final List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();
    ManagedRocksDB rocksDB = ManagedRocksDB.openReadOnly(parent.getDbPath(),
            cfs, columnFamilyHandleList);
    this.printAppropriateTable(columnFamilyHandleList,
           rocksDB, parent.getDbPath());
    return null;
  }

  private void printAppropriateTable(
          List<ColumnFamilyHandle> columnFamilyHandleList,
          ManagedRocksDB rocksDB, String dbPath) throws IOException {
    if (limit < 1 && limit != -1) {
      throw new IllegalArgumentException(
              "List length should be a positive number. Only allowed negative" +
                  " number is -1 which is to dump entire table");
    }
    dbPath = removeTrailingSlashIfNeeded(dbPath);
    DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
    this.constructColumnFamilyMap(DBDefinitionFactory.
            getDefinition(Paths.get(dbPath), new OzoneConfiguration()));
    if (this.columnFamilyMap != null) {
      if (!this.columnFamilyMap.containsKey(tableName)) {
        System.out.print("Table with name:" + tableName + " does not exist");
      } else {
        DBColumnFamilyDefinition columnFamilyDefinition =
                this.columnFamilyMap.get(tableName);
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
                columnFamilyDefinition.getTableName()
                        .getBytes(StandardCharsets.UTF_8),
                columnFamilyHandleList);
        if (columnFamilyHandle == null) {
          throw new IllegalArgumentException("columnFamilyHandle is null");
        }
        ManagedRocksIterator iterator;
        if (containerId > 0 && dnDBSchemaVersion != null &&
            dnDBSchemaVersion.equals("V3")) {
          ManagedReadOptions readOptions = new ManagedReadOptions();
          readOptions.setIterateUpperBound(new ManagedSlice(
              FixedLengthStringUtils.string2Bytes(
                  DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix(
                  containerId + 1))));
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
        scannedObjects = displayTable(iterator, columnFamilyDefinition);
      }
    } else {
      System.out.println("Incorrect db Path");
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

