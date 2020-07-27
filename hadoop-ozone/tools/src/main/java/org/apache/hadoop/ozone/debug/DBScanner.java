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

  private HashMap<String, DBColumnFamilyDefinition> columnFamilyMap;

  private static void displayTable(RocksDB rocksDB,
        DBColumnFamilyDefinition dbColumnFamilyDefinition,
        List<ColumnFamilyHandle> list) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
            dbColumnFamilyDefinition.getTableName()
                    .getBytes(StandardCharsets.UTF_8), list);
    if (columnFamilyHandle==null){
      throw new IllegalArgumentException("columnFamilyHandle is null");
    }
    RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle);
    iterator.seekToFirst();
    while (iterator.isValid()){
      Object o = dbColumnFamilyDefinition.getValueCodec()
              .fromPersistedFormat(iterator.value());
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      String result = gson.toJson(o);
      System.out.println(result);
      iterator.next();
    }
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
           rocksDB, parent.getDbPath());
    return null;
  }

  private void printAppropriateTable(
          List<ColumnFamilyHandle> columnFamilyHandleList,
          RocksDB rocksDB, String dbPath) throws IOException {
    dbPath = removeTrailingSlashIfNeeded(dbPath);
    this.constructColumnFamilyMap(DBDefinitionFactory.
            getDefinition(new File(dbPath).getName()));
    if (this.columnFamilyMap !=null) {
      if (!this.columnFamilyMap.containsKey(tableName)) {
        System.out.print("Table with specified name does not exist");
      } else {
        DBColumnFamilyDefinition columnFamilyDefinition =
                this.columnFamilyMap.get(tableName);
        displayTable(rocksDB, columnFamilyDefinition, columnFamilyHandleList);
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
