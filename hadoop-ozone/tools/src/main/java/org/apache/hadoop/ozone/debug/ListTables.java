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

import org.rocksdb.*;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * List all column Families/Tables in db.
 */
@CommandLine.Command(
        name = "list",
        description = "list all tables in db."
)
public class ListTables implements Callable<Void> {

  @CommandLine.ParentCommand
  private RDBParser parent;

  @Override
  public Void call() throws Exception {
    List<byte[]> cfList = getColumnFamilyList(parent.getDbPath());
    for (byte[] b : cfList) {
      System.out.println(new String(b, StandardCharsets.UTF_8));
    }
    return null;
  }

  private static List<byte[]> getColumnFamilyList(String dbPath) {
    List<byte[]> cfList = null;
    try {
      cfList = RocksDB.listColumnFamilies(new Options(), dbPath);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
    return cfList;
  }
}
