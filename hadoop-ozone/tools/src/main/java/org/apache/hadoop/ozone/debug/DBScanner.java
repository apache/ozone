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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.OzoneConsts;
import org.rocksdb.*;
import picocli.CommandLine;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Parser for scm.db file.
 */
@CommandLine.Command(
        name = "scan",
        description = "Parse specified metadataTable"
)
public class DBScanner implements Callable<Void> {

  @CommandLine.Option(names = {"--column_family"},
            description = "Table name")
  private String tableName;

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"--length", "-l"},
          description = "Maximum number of items to list")
  private int limit = 100;

  private HashMap<String, DBColumnFamilyDefinition> columnFamilyMap;

  private List<Object> scannedObjects;

  private static List<Object> displayTable(RocksDB rocksDB,
      DBColumnFamilyDefinition dbColumnFamilyDefinition,
      List<ColumnFamilyHandle> list, int maxValueLimit) throws IOException {
    List<Object> outputs = new ArrayList<>();
    ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
            dbColumnFamilyDefinition.getTableName()
                    .getBytes(StandardCharsets.UTF_8), list);
    if (columnFamilyHandle==null){
      throw new IllegalArgumentException("columnFamilyHandle is null");
    }
    RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle);
    iterator.seekToFirst();
    while (iterator.isValid() && maxValueLimit > 0){
      Object o = dbColumnFamilyDefinition.getValueCodec()
              .fromPersistedFormat(iterator.value());
      outputs.add(o);
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      String result = gson.toJson(o);
      System.out.println(result);
      maxValueLimit--;
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

  public void setLimit(int limit) {
    this.limit = limit;
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
    for(DBColumnFamilyDefinition definition:columnFamilyDefinitions){
      this.columnFamilyMap.put(definition.getTableName(), definition);
    }
  }

  public List<Object> getScannedObjects() {
    return scannedObjects;
  }

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    final List<ColumnFamilyHandle> columnFamilyHandleList =
            new ArrayList<>();
    List<byte[]> cfList = null;
    cfList = RocksDB.listColumnFamilies(new Options(),
            parent.getDbPath());
    if (cfList != null) {
      for (byte[] b : cfList) {
        cfs.add(new ColumnFamilyDescriptor(b));
      }
    }
    RocksDB rocksDB = null;
    rocksDB = RocksDB.openReadOnly(parent.getDbPath(),
            cfs, columnFamilyHandleList);
    this.printAppropriateTable(columnFamilyHandleList,
           rocksDB, parent.getDbPath(), limit);
    return null;
  }

  private void printAppropriateTable(
          List<ColumnFamilyHandle> columnFamilyHandleList,
          RocksDB rocksDB, String dbPath, int maxValues) throws IOException {
    if (maxValues < 1) {
      throw new IllegalArgumentException(
              "List length should be a positive number");
    }
    dbPath = removeTrailingSlashIfNeeded(dbPath);
    this.constructColumnFamilyMap(DBDefinitionFactory.
            getDefinition(new File(dbPath).getName()));
    if (this.columnFamilyMap !=null) {
      if (!this.columnFamilyMap.containsKey(tableName)) {
        System.out.print("Table with specified name does not exist");
      } else {
        DBColumnFamilyDefinition columnFamilyDefinition =
                this.columnFamilyMap.get(tableName);
        scannedObjects = displayTable(rocksDB,
                columnFamilyDefinition, columnFamilyHandleList, maxValues);
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
}
