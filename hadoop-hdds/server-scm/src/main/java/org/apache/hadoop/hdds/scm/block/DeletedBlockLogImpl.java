/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.block;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataKeyFilters.MetadataKeyFilter;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.DELETED_BLOCK_DB;

/**
 * A implement class of {@link DeletedBlockLog}, and it uses
 * K/V db to maintain block deletion transactions between scm and datanode.
 * This is a very basic implementation, it simply scans the log and
 * memorize the position that scanned by last time, and uses this to
 * determine where the next scan starts. It has no notion about weight
 * of each transaction so as long as transaction is still valid, they get
 * equally same chance to be retrieved which only depends on the nature
 * order of the transaction ID.
 */
public class DeletedBlockLogImpl implements DeletedBlockLog {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogImpl.class);

  private static final byte[] LATEST_TXID =
      DFSUtil.string2Bytes("#LATEST_TXID#");

  private final int maxRetry;
  private final MetadataStore deletedStore;
  private final Lock lock;
  // The latest id of deleted blocks in the db.
  private long lastTxID;
  private long lastReadTxID;

  public DeletedBlockLogImpl(Configuration conf) throws IOException {
    maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY,
        OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT);

    File metaDir = getOzoneMetaDirPath(conf);
    String scmMetaDataDir = metaDir.getPath();
    File deletedLogDbPath = new File(scmMetaDataDir, DELETED_BLOCK_DB);
    int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    // Load store of all transactions.
    deletedStore = MetadataStoreBuilder.newBuilder()
        .setCreateIfMissing(true)
        .setConf(conf)
        .setDbFile(deletedLogDbPath)
        .setCacheSize(cacheSize * OzoneConsts.MB)
        .build();

    this.lock = new ReentrantLock();
    // start from the head of deleted store.
    lastReadTxID = 0;
    lastTxID = findLatestTxIDInStore();
  }

  @VisibleForTesting
  MetadataStore getDeletedStore() {
    return deletedStore;
  }

  /**
   * There is no need to lock before reading because
   * it's only used in construct method.
   *
   * @return latest txid.
   * @throws IOException
   */
  private long findLatestTxIDInStore() throws IOException {
    long txid = 0;
    byte[] value = deletedStore.get(LATEST_TXID);
    if (value != null) {
      txid = Longs.fromByteArray(value);
    }
    return txid;
  }

  @Override
  public List<DeletedBlocksTransaction> getTransactions(
      int count) throws IOException {
    List<DeletedBlocksTransaction> result = new ArrayList<>();
    MetadataKeyFilter getNextTxID = (preKey, currentKey, nextKey)
        -> Longs.fromByteArray(currentKey) > lastReadTxID;
    MetadataKeyFilter avoidInvalidTxid = (preKey, currentKey, nextKey)
        -> !Arrays.equals(LATEST_TXID, currentKey);
    lock.lock();
    try {
      deletedStore.iterate(null, (key, value) -> {
        if (getNextTxID.filterKey(null, key, null) &&
            avoidInvalidTxid.filterKey(null, key, null)) {
          DeletedBlocksTransaction block = DeletedBlocksTransaction
              .parseFrom(value);
          if (block.getCount() > -1 && block.getCount() <= maxRetry) {
            result.add(block);
          }
        }
        return result.size() < count;
      });
      // Scan the metadata from the beginning.
      if (result.size() < count || result.size() < 1) {
        lastReadTxID = 0;
      } else {
        lastReadTxID = result.get(result.size() - 1).getTxID();
      }
    } finally {
      lock.unlock();
    }
    return result;
  }

  @Override
  public List<DeletedBlocksTransaction> getFailedTransactions()
      throws IOException {
    lock.lock();
    try {
      final List<DeletedBlocksTransaction> failedTXs = Lists.newArrayList();
      deletedStore.iterate(null, (key, value) -> {
        if (!Arrays.equals(LATEST_TXID, key)) {
          DeletedBlocksTransaction delTX =
              DeletedBlocksTransaction.parseFrom(value);
          if (delTX.getCount() == -1) {
            failedTXs.add(delTX);
          }
        }
        return true;
      });
      return failedTXs;
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param txIDs - transaction ID.
   * @throws IOException
   */
  @Override
  public void incrementCount(List<Long> txIDs) throws IOException {
    BatchOperation batch = new BatchOperation();
    lock.lock();
    try {
      for(Long txID : txIDs) {
        try {
          byte[] deleteBlockBytes =
              deletedStore.get(Longs.toByteArray(txID));
          if (deleteBlockBytes == null) {
            LOG.warn("Delete txID {} not found", txID);
            continue;
          }
          DeletedBlocksTransaction block = DeletedBlocksTransaction
              .parseFrom(deleteBlockBytes);
          DeletedBlocksTransaction.Builder builder = block.toBuilder();
          int currentCount = block.getCount();
          if (currentCount > -1) {
            builder.setCount(++currentCount);
          }
          // if the retry time exceeds the maxRetry value
          // then set the retry value to -1, stop retrying, admins can
          // analyze those blocks and purge them manually by SCMCli.
          if (currentCount > maxRetry) {
            builder.setCount(-1);
          }
          deletedStore.put(Longs.toByteArray(txID),
              builder.build().toByteArray());
        } catch (IOException ex) {
          LOG.warn("Cannot increase count for txID " + txID, ex);
        }
      }
      deletedStore.writeBatch(batch);
    } finally {
      lock.unlock();
    }
  }

  private DeletedBlocksTransaction constructNewTransaction(long txID,
      long containerID, List<Long> blocks) {
    return DeletedBlocksTransaction.newBuilder()
        .setTxID(txID)
        .setContainerID(containerID)
        .addAllLocalID(blocks)
        .setCount(0)
        .build();
  }

  /**
   * {@inheritDoc}
   *
   * @param txIDs - transaction IDs.
   * @throws IOException
   */
  @Override
  public void commitTransactions(List<Long> txIDs) throws IOException {
    lock.lock();
    try {
      for (Long txID : txIDs) {
        try {
          deletedStore.delete(Longs.toByteArray(txID));
        } catch (IOException ex) {
          LOG.warn("Cannot commit txID " + txID, ex);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param containerID - container ID.
   * @param blocks - blocks that belong to the same container.
   * @throws IOException
   */
  @Override
  public void addTransaction(long containerID, List<Long> blocks)
      throws IOException {
    BatchOperation batch = new BatchOperation();
    lock.lock();
    try {
      DeletedBlocksTransaction tx = constructNewTransaction(lastTxID + 1,
          containerID, blocks);
      byte[] key = Longs.toByteArray(lastTxID + 1);

      batch.put(key, tx.toByteArray());
      batch.put(LATEST_TXID, Longs.toByteArray(lastTxID + 1));

      deletedStore.writeBatch(batch);
      lastTxID += 1;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int getNumOfValidTransactions() throws IOException {
    lock.lock();
    try {
      final AtomicInteger num = new AtomicInteger(0);
      deletedStore.iterate(null, (key, value) -> {
        // Exclude latest txid record
        if (!Arrays.equals(LATEST_TXID, key)) {
          DeletedBlocksTransaction delTX =
              DeletedBlocksTransaction.parseFrom(value);
          if (delTX.getCount() > -1) {
            num.incrementAndGet();
          }
        }
        return true;
      });
      return num.get();
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param containerBlocksMap a map of containerBlocks.
   * @return Mapping from containerId to latest transactionId for the container.
   * @throws IOException
   */
  @Override
  public Map<Long, Long> addTransactions(
      Map<Long, List<Long>> containerBlocksMap)
      throws IOException {
    BatchOperation batch = new BatchOperation();
    Map<Long, Long> deleteTransactionsMap = new HashMap<>();
    lock.lock();
    try {
      long currentLatestID = lastTxID;
      for (Map.Entry<Long, List<Long>> entry :
          containerBlocksMap.entrySet()) {
        currentLatestID += 1;
        byte[] key = Longs.toByteArray(currentLatestID);
        DeletedBlocksTransaction tx = constructNewTransaction(currentLatestID,
            entry.getKey(), entry.getValue());
        deleteTransactionsMap.put(entry.getKey(), currentLatestID);
        batch.put(key, tx.toByteArray());
      }
      lastTxID = currentLatestID;
      batch.put(LATEST_TXID, Longs.toByteArray(lastTxID));
      deletedStore.writeBatch(batch);
      return deleteTransactionsMap;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (deletedStore != null) {
      deletedStore.close();
    }
  }

  @Override
  public void getTransactions(DatanodeDeletedBlockTransactions transactions)
      throws IOException {
    lock.lock();
    try {
      deletedStore.iterate(null, (key, value) -> {
        if (!Arrays.equals(LATEST_TXID, key)) {
          DeletedBlocksTransaction block = DeletedBlocksTransaction
              .parseFrom(value);

          if (block.getCount() > -1 && block.getCount() <= maxRetry) {
            transactions.addTransaction(block);
          }
          return !transactions.isFull();
        }
        return true;
      });
    } finally {
      lock.unlock();
    }
  }
}
