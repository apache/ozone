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

import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.rocksdb.*;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Parser for scm.db file.
 */
@CommandLine.Command(
        name = "scmdbparser",
        description = "Parse specified metadataTable"
)
public class SCMDBParser implements Callable<Void> {

  @CommandLine.Option(names = {"-table"},
            description = "Table name")
  private String tableName;

  @CommandLine.ParentCommand
  private RDBParser parent;


  private static void displayTable(RocksDB rocksDB,
        DBColumnFamilyDefinition dbColumnFamilyDefinition,
        List<ColumnFamilyHandle> list) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
            dbColumnFamilyDefinition.getTableName()
                    .getBytes(StandardCharsets.UTF_8), list);
    RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle);
    iterator.seekToFirst();
    while (iterator.isValid()){
      Object o = dbColumnFamilyDefinition.getValueCodec()
              .fromPersistedFormat(iterator.value());
      System.out.println(o);
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

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    final List<ColumnFamilyHandle> columnFamilyHandleList =
            new ArrayList<>();
    List<byte[]> cfList = null;
    try {
      cfList = RocksDB.listColumnFamilies(new Options(),
                    parent.getDbPath());
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (cfList != null) {
      for (byte[] b : cfList) {
        cfs.add(new ColumnFamilyDescriptor(b));
      }
    }
    RocksDB rocksDB = null;
    try {
      rocksDB = RocksDB.openReadOnly(parent.getDbPath(),
              cfs, columnFamilyHandleList);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
    printAppropriateTable(columnFamilyHandleList, rocksDB);
    return null;
  }

  private void printAppropriateTable(
      List<ColumnFamilyHandle> columnFamilyHandleList,
      RocksDB rocksDB) throws IOException {
    if (!RDBParser.getColumnFamilyMap().containsKey(tableName)){
      System.out.print("Table with specified name does not exist");
    } else {
      DBColumnFamilyDefinition columnFamilyDefinition =
              RDBParser.getColumnFamilyMap().get(tableName);
      displayTable(rocksDB, columnFamilyDefinition, columnFamilyHandleList);
    }
  }
}
