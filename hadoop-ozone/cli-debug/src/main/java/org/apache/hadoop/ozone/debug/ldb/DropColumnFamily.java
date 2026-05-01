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

import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Drop a column family from a RocksDB.
 */
@CommandLine.Command(
    name = "drop-column-family",
    description = "Drop a column family from the DB.")
public class DropColumnFamily extends AbstractSubcommand
    implements Callable<Void> {

  private static final String WARNING_TO_STOP_SERVICE =
      "WARNING: Dropping a column family mutates RocksDB and can be unsafe " +
          "if the DB is already open by an Ozone process. Ensure the service " +
          "that owns this DB is stopped before running this command. " +
          "Do you want to continue (y/N)? ";

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"--column-family", "--cf"},
      required = true,
      description = "Column family name")
  private String columnFamilyName;

  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;

  @Override
  public Void call() throws Exception {
    if (isDefaultColumnFamily(columnFamilyName)) {
      throw new IllegalArgumentException(
          "Default column family cannot be dropped.");
    }
    if (!columnFamilyExists()) {
      throw new IllegalArgumentException(columnFamilyName
          + " is not a column family in DB for the given path.");
    }

    confirm();

    ManagedConfigOptions configOptions = new ManagedConfigOptions();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();

    try (ManagedRocksDB db = ManagedRocksDB.openWithLatestOptions(
        configOptions, dbOptions, parent.getDbPath(), cfDescList,
        cfHandleList)) {
      ColumnFamilyHandle columnFamilyHandle =
          RocksDBUtils.getColumnFamilyHandle(columnFamilyName, cfHandleList);
      if (columnFamilyHandle == null) {
        throw new IllegalArgumentException(columnFamilyName
            + " is not a column family in DB for the given path.");
      }

      db.get().dropColumnFamily(columnFamilyHandle);
      out().println("Dropped column family " + columnFamilyName);
    } catch (RocksDBException exception) {
      String errorMessage = "Failed to drop column family " + columnFamilyName
          + " from RocksDB for the given path: " + parent.getDbPath();
      throw new IOException(errorMessage, exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
      closeDescriptors(cfDescList);
      IOUtils.closeQuietly(configOptions, dbOptions);
    }

    return null;
  }

  private static boolean isDefaultColumnFamily(String name) {
    return DEFAULT_COLUMN_FAMILY_NAME.equals(name);
  }

  private void confirm() {
    if (yes) {
      return;
    }

    err().print(WARNING_TO_STOP_SERVICE);
    err().flush();
    Scanner scanner = new Scanner(new InputStreamReader(
        System.in, StandardCharsets.UTF_8));
    String confirmation = scanner.hasNextLine()
        ? scanner.nextLine().trim() : "";
    if (!"y".equalsIgnoreCase(confirmation)) {
      throw new IllegalStateException("Aborting command.");
    }
  }

  private boolean columnFamilyExists() throws RocksDBException {
    for (byte[] columnFamily : RocksDatabase.listColumnFamiliesEmptyOptions(
        parent.getDbPath())) {
      if (columnFamilyName.equals(new String(columnFamily,
          StandardCharsets.UTF_8))) {
        return true;
      }
    }
    return false;
  }

  private static void closeDescriptors(
      List<ColumnFamilyDescriptor> descriptors) {
    descriptors.forEach(descriptor -> descriptor.getOptions().close());
  }
}
