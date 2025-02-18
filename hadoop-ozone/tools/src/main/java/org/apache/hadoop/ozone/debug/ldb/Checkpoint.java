/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.debug.ldb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCheckpoint;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import picocli.CommandLine;

/**
 * Create a checkpoint for a rocksDB.
 */
@CommandLine.Command(
    name = "checkpoint",
    description = "Create checkpoint for specified db"
)
public class Checkpoint extends AbstractSubcommand implements Callable<Void> {
  @CommandLine.Option(names = {"--output"},
      required = true,
      description = "Path to output directory for the checkpoint.")
  private String outputPath;

  @CommandLine.ParentCommand
  private RDBParser parent;

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyDescriptor> cfDescList =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
    final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();

    // Create checkpoint
    try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(
        parent.getDbPath(), cfDescList, cfHandleList)) {
      ManagedCheckpoint cp = ManagedCheckpoint.create(db);
      cp.get().createCheckpoint(outputPath);
      out().println("Created checkpoint at " + outputPath);
    }
    return null;
  }
}
