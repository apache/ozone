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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.OzoneConsts;

import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import picocli.CommandLine;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Parser for scm.db file.
 */
@CommandLine.Command(
        name = "scan",
        description = "Parse specified metadataTable"
)
@MetaInfServices(SubcommandWithParent.class)
public class DBScanner implements Callable<Void>, SubcommandWithParent {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--column_family"},
      required = true,
      description = "Table name")
  private String tableName;

  @Deprecated
  @CommandLine.Option(names = {"--with-keys"},
      description = "[ignored]",
      defaultValue = "false")
  private boolean withKeys;

  @CommandLine.Option(names = {"--omit-keys"},
      description = "Print only values",
      defaultValue = "false")
  private boolean omitKeys;

  @CommandLine.Option(names = {"--length", "-l"},
          description = "Maximum number of items to list. " +
              "If -1 dumps the entire table data")
  private int limit = 100;

  @CommandLine.Option(names = {"--out", "-o"},
      description = "File to dump table scan data")
  private String fileName;

  @CommandLine.Option(names = {"--dnSchema", "-d"},
      description = "Datanode DB Schema Version : V1/V2",
      defaultValue = "V2")
  private String dnDBSchemaVersion;

  @CommandLine.ParentCommand
  private RDBParser parent;

  private HashMap<String, DBColumnFamilyDefinition> columnFamilyMap;

  private PrintWriter err() {
    return spec.commandLine().getErr();
  }

  private PrintWriter out() {
    return spec.commandLine().getOut();
  }

  private void displayTable(RocksIterator iterator,
      DBColumnFamilyDefinition dbColumnFamilyDefinition) throws IOException {
    iterator.seekToFirst();

    if (fileName != null) {
      try (PrintWriter out = new PrintWriter(fileName, UTF_8.name())) {
        displayTable(iterator, dbColumnFamilyDefinition, out);
      }
    } else {
      displayTable(iterator, dbColumnFamilyDefinition, out());
    }
  }

  private void displayTable(RocksIterator iter,
      DBColumnFamilyDefinition dbColumnFamilyDefinition, PrintWriter out)
      throws IOException {
    Object result;
    if (omitKeys) {
      List<Object> list = new ArrayList<>();
      for (int i = 0; iter.isValid() && withinLimit(i); iter.next(), i++) {
        list.add(getValue(iter, dbColumnFamilyDefinition));
      }
      result = list;
    } else {
      Map<Object, Object> map = new LinkedHashMap<>();
      for (int i = 0; iter.isValid() && withinLimit(i); iter.next(), i++) {
        Object k = getKey(iter, dbColumnFamilyDefinition);
        Object v = getValue(iter, dbColumnFamilyDefinition);
        map.put(k, v);
      }
      result = map;
    }
    out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(result));
  }

  private boolean withinLimit(int i) {
    return limit == -1 || i < limit;
  }

  private Object getKey(RocksIterator iterator,
      DBColumnFamilyDefinition dbColumnFamilyDefinition) throws IOException {
    return dbColumnFamilyDefinition.getKeyCodec()
        .fromPersistedFormat(iterator.key());
  }

  private Object getValue(RocksIterator iterator,
      DBColumnFamilyDefinition dbColumnFamilyDefinition) throws IOException {
    return dbColumnFamilyDefinition.getValueCodec()
            .fromPersistedFormat(iterator.value());
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

  private void constructColumnFamilyMap(DBDefinition dbDefinition) {
    if (dbDefinition == null) {
      System.out.println("Incorrect Db Path");
      return;
    }
    columnFamilyMap = new HashMap<>();
    DBColumnFamilyDefinition[] columnFamilyDefinitions = dbDefinition
            .getColumnFamilies();
    for (DBColumnFamilyDefinition definition:columnFamilyDefinitions) {
      err().println("Added definition for table:" +
          definition.getTableName());
      columnFamilyMap.put(definition.getTableName(), definition);
    }
  }

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyDescriptor> cfs =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());

    final List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();
    RocksDB rocksDB = RocksDB.openReadOnly(parent.getDbPath(),
            cfs, columnFamilyHandleList);
    printAppropriateTable(columnFamilyHandleList, rocksDB, parent.getDbPath());
    return null;
  }

  private void printAppropriateTable(
          List<ColumnFamilyHandle> columnFamilyHandleList,
          RocksDB rocksDB, String dbPath) throws IOException {
    if (limit < 1 && limit != -1) {
      throw new IllegalArgumentException(
              "List length should be a positive number. Only allowed negative" +
                  " number is -1 which is to dump entire table");
    }
    Path path = Paths.get(removeTrailingSlashIfNeeded(dbPath));
    DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
    constructColumnFamilyMap(DBDefinitionFactory.getDefinition(path));
    if (columnFamilyMap != null) {
      if (!columnFamilyMap.containsKey(tableName)) {
        err().print("Table with name:" + tableName + " does not exist");
      } else {
        DBColumnFamilyDefinition columnFamilyDefinition =
                columnFamilyMap.get(tableName);
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
                columnFamilyDefinition.getTableName()
                        .getBytes(UTF_8),
                columnFamilyHandleList);
        if (columnFamilyHandle == null) {
          throw new IllegalArgumentException("columnFamilyHandle is null");
        }
        RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle);
        displayTable(iterator, columnFamilyDefinition);
      }
    } else {
      err().println("Incorrect db Path");
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

