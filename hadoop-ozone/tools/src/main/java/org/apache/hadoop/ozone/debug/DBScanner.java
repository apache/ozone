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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.OzoneConsts;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import picocli.CommandLine;

/**
 * Parser for scm.db file.
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
          description = "Maximum number of items to list")
  private static int limit = 100;

  @CommandLine.ParentCommand
  private RDBParser parent;

  private HashMap<String, DBColumnFamilyDefinition> columnFamilyMap;

  private List<Object> scannedObjects;

  private static List<Object> displayTable(RocksIterator iterator,
      DBColumnFamilyDefinition dbColumnFamilyDefinition) throws IOException {
    List<Object> outputs = new ArrayList<>();
    iterator.seekToFirst();
    while (iterator.isValid() && limit > 0){
      StringBuilder result = new StringBuilder();
      if (withKey) {
        Object key = dbColumnFamilyDefinition.getKeyCodec()
            .fromPersistedFormat(iterator.key());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        result.append(gson.toJson(key));
        result.append(" -> ");
      }
      Object o = dbColumnFamilyDefinition.getValueCodec()
              .fromPersistedFormat(iterator.value());
      outputs.add(o);
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      result.append(gson.toJson(o));
      System.out.println(result.toString());
      limit--;
      iterator.next();
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
    if (dbDefinition == null){
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
    RocksDB rocksDB = RocksDB.openReadOnly(parent.getDbPath(),
            cfs, columnFamilyHandleList);
    this.printAppropriateTable(columnFamilyHandleList,
           rocksDB, parent.getDbPath());
    return null;
  }

  private void printAppropriateTable(
          List<ColumnFamilyHandle> columnFamilyHandleList,
          RocksDB rocksDB, String dbPath) throws IOException {
    if (limit < 1) {
      throw new IllegalArgumentException(
              "List length should be a positive number");
    }
    dbPath = removeTrailingSlashIfNeeded(dbPath);
    this.constructColumnFamilyMap(DBDefinitionFactory.
            getDefinition(new File(dbPath).getName()));
    if (this.columnFamilyMap !=null) {
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
        RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle);
        scannedObjects = displayTable(iterator, columnFamilyDefinition);
      }
    } else {
      System.out.println("Incorrect db Path");
    }
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if(dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)){
      dbPath = dbPath.substring(0, dbPath.length()-1);
    }
    return dbPath;
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }
}

