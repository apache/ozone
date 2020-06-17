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

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Create a column Family/Table in db.
 */
@CommandLine.Command(
        name = "create_column_family",
        aliases = "create",
        description = "create column family in db."
)
public class CreateTable implements Callable<Void> {

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"-n", "--name"},
      description = "column family name.")
  private String name;

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    final List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();
    List<byte[]> cfList = RocksDB.listColumnFamilies(new Options(),
        parent.getDbPath());
    if (cfList != null) {
      for (byte[] b : cfList) {
        cfs.add(new ColumnFamilyDescriptor(b));
      }
    }
    try (RocksDB rocksDB = RocksDB.open(
        parent.getDbPath(), cfs, columnFamilyHandleList)) {
      rocksDB.createColumnFamily(new ColumnFamilyDescriptor(
          name.getBytes(StandardCharsets.UTF_8)));
    }
    return null;
  }
}