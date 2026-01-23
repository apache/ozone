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

package org.apache.hadoop.ozone.repair;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Tool to update the highest term-index in transaction info table.
 */
@CommandLine.Command(
    name = "update-transaction",
    description = "CLI to update the highest index in transaction info table." +
        " The corresponding OM or SCM role should be stopped for this tool.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class TransactionInfoRepair extends RepairTool {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  @CommandLine.Option(names = {"--term"},
      required = true,
      description = "Highest term to set. The input should be non-zero long integer.")
  private long highestTransactionTerm;

  @CommandLine.Option(names = {"--index"},
      required = true,
      description = "Highest index to set. The input should be non-zero long integer.")
  private long highestTransactionIndex;

  @Override
  public void execute() throws Exception {
    ManagedConfigOptions configOptions = new ManagedConfigOptions();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();
    String columnFamilyName = getColumnFamily(serviceToBeOffline()).getName();

    try (ManagedRocksDB db = ManagedRocksDB.openWithLatestOptions(
        configOptions, dbOptions, dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle transactionInfoCfh = RocksDBUtils.getColumnFamilyHandle(columnFamilyName, cfHandleList);
      if (transactionInfoCfh == null) {
        throw new IllegalArgumentException(columnFamilyName +
            " is not in a column family in DB for the given path.");
      }
      TransactionInfo originalTransactionInfo =
          RocksDBUtils.getValue(db, transactionInfoCfh, TRANSACTION_INFO_KEY, TransactionInfo.getCodec());

      info("The original highest transaction Info was %s", originalTransactionInfo.getTermIndex());

      TransactionInfo transactionInfo = TransactionInfo.valueOf(highestTransactionTerm, highestTransactionIndex);

      byte[] transactionInfoBytes = TransactionInfo.getCodec().toPersistedFormat(transactionInfo);
      byte[] key = StringCodec.get().toPersistedFormat(TRANSACTION_INFO_KEY);

      info("Updating transaction info to %s", transactionInfo.getTermIndex());

      if (!isDryRun()) {
        db.get().put(transactionInfoCfh, key, transactionInfoBytes);

        info("The highest transaction info has been updated to: %s",
            RocksDBUtils.getValue(db, transactionInfoCfh, TRANSACTION_INFO_KEY,
                TransactionInfo.getCodec()).getTermIndex());
      }
    } catch (RocksDBException exception) {
      error("Failed to update the RocksDB for the given path: %s", dbPath);
      throw new IOException("Failed to update RocksDB.", exception);
    } finally {
      IOUtils.closeQuietly(configOptions);
      IOUtils.closeQuietly(dbOptions);
      IOUtils.closeQuietly(cfHandleList);
    }
  }

  @Override
  @Nonnull
  protected Component serviceToBeOffline() {
    final String parent = spec().parent().name();
    switch (parent) {
    case "om":
      return Component.OM;
    case "scm":
      return Component.SCM;
    default:
      throw new IllegalStateException("Unknown component: " + parent);
    }
  }

  public static DBColumnFamilyDefinition<String, TransactionInfo> getColumnFamily(Component component) {
    switch (component) {
    case OM:
      return OMDBDefinition.TRANSACTION_INFO_TABLE_DEF;
    case SCM:
      return SCMDBDefinition.TRANSACTIONINFO;
    default:
      throw new IllegalStateException("This tool does not support component: " + component);
    }
  }
}
