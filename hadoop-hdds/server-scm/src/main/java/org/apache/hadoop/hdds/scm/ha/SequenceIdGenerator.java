/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.utils.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.SEQUENCE_ID;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SEQUENCE_ID_BATCH_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SEQUENCE_ID_BATCH_SIZE_DEFAULT;

/**
 * After SCM starts, set lastId = 0, nextId = lastId + 1.
 * The first getNextId() call triggers SCM to load lastId from rocksDB,
 * and allocate a new batch.
 *
 * In order to maintain monotonicity, for Ratis based SequenceIdGen,
 * when becoming leader, SCM invalidates un-exhausted id batch by setting
 * nextId = lastId + 1, so that a new leader will reload lastId from
 * rocksDB and allocate a new batch when receiving its first getNextId() call.
 */
public class SequenceIdGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(SequenceIdGenerator.class);
  private static final long INVALID_SEQUENCE_ID = 0;

  static class Batch {
    // The upper bound of the batch.
    private long lastId = INVALID_SEQUENCE_ID;
    // The next id to be allocated in this batch.
    private long nextId = lastId + 1;
  }

  private final Map<String, Batch> sequenceIdToBatchMap;

  private final Lock lock;
  private final long batchSize;
  private final StateManager stateManager;

  /**
   * @param conf            : conf
   * @param scmhaManager    : null if non-Ratis based
   * @param sequenceIdTable : sequenceIdTable
   */
  public SequenceIdGenerator(ConfigurationSource conf,
      SCMHAManager scmhaManager, Table<String, Long> sequenceIdTable) {
    this.sequenceIdToBatchMap = new HashMap<>();
    this.lock = new ReentrantLock();
    this.batchSize = conf.getInt(OZONE_SCM_SEQUENCE_ID_BATCH_SIZE,
        OZONE_SCM_SEQUENCE_ID_BATCH_SIZE_DEFAULT);

    if (SCMHAUtils.isSCMHAEnabled(conf)) {
      this.stateManager = new StateManagerHAImpl.Builder()
          .setRatisServer(scmhaManager.getRatisServer())
          .setDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
          .setSequenceIdTable(sequenceIdTable)
          .build();
    } else {
      this.stateManager = new StateManagerImpl(sequenceIdTable);
    }
  }

  /**
   * @param sequenceIdName : name of the sequenceId
   * @return : next id of this sequenceId.
   */
  public long getNextId(String sequenceIdName) {
    lock.lock();
    try {
      Batch batch = sequenceIdToBatchMap.computeIfAbsent(
          sequenceIdName, key -> new Batch());

      if (batch.nextId <= batch.lastId) {
        return batch.nextId++;
      }

      Preconditions.checkArgument(batch.nextId == batch.lastId + 1);
      while (true) {
        Long prevLastId = batch.lastId;
        batch.nextId = prevLastId + 1;

        Preconditions.checkArgument(Long.MAX_VALUE - batch.lastId >= batchSize);
        batch.lastId += batchSize;

        if (stateManager.allocateBatch(sequenceIdName,
            prevLastId, batch.lastId)) {
          LOG.info("Allocate a batch for {}, change lastId from {} to {}.",
              sequenceIdName, prevLastId, batch.lastId);
          break;
        }

        // reload lastId from RocksDB.
        batch.lastId = stateManager.getLastId(sequenceIdName);
      }

      Preconditions.checkArgument(batch.nextId <= batch.lastId);
      return batch.nextId++;

    } finally {
      lock.unlock();
    }
  }

  /**
   * Invalidate any un-exhausted batch, next getNextId() call will
   * allocate a new batch.
   */
  public void invalidateBatch() {
    lock.lock();
    try {
      sequenceIdToBatchMap.forEach(
          (sequenceId, batch) -> batch.nextId = batch.lastId + 1);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Maintain SequenceIdTable in RocksDB.
   */
  interface StateManager {
    /**
     * Compare And Swap lastId saved in db from expectedLastId to newLastId.
     * If based on Ratis, it will submit a raft client request.
     *
     * @param sequenceIdName : name of the sequence id.
     * @param expectedLastId : the expected lastId saved in db
     * @param newLastId      : the new lastId to save in db
     * @return               : result of the C.A.S.
     */
    @Replicate
    Boolean allocateBatch(String sequenceIdName,
                          Long expectedLastId, Long newLastId);

    /**
     * @param sequenceIdName : name of the sequence id.
     * @return lastId saved in db
     */
    Long getLastId(String sequenceIdName);
  }

  /**
   * Ratis based StateManager, db operations are queued in
   * DBTransactionBuffer until a snapshot is taken.
   */
  static final class StateManagerHAImpl implements StateManager {
    private final Table<String, Long> sequenceIdTable;
    private final DBTransactionBuffer transactionBuffer;
    private final Map<String, Long> sequenceIdToLastIdMap;

    private StateManagerHAImpl(Table<String, Long> sequenceIdTable,
                               DBTransactionBuffer trxBuffer) {
      this.sequenceIdTable = sequenceIdTable;
      this.transactionBuffer = trxBuffer;
      this.sequenceIdToLastIdMap = new ConcurrentHashMap<>();
      LOG.info("Init the HA SequenceIdGenerator.");
    }

    @Override
    public Boolean allocateBatch(String sequenceIdName,
                                 Long expectedLastId, Long newLastId) {
      Long lastId = sequenceIdToLastIdMap.computeIfAbsent(sequenceIdName,
          key -> {
            try {
              Long idInDb = this.sequenceIdTable.get(key);
              return idInDb != null ? idInDb : INVALID_SEQUENCE_ID;
            } catch (IOException ioe) {
              throw new RuntimeException("Failed to get lastId from db", ioe);
            }
          });

      if (!lastId.equals(expectedLastId)) {
        LOG.warn("Failed to allocate a batch for {}, expected lastId is {}," +
            " actual lastId is {}.", sequenceIdName, expectedLastId, lastId);
        return false;
      }

      try {
        sequenceIdTable.putWithBatch(transactionBuffer
            .getCurrentBatchOperation(), sequenceIdName, newLastId);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to put lastId to Batch", ioe);
      }

      sequenceIdToLastIdMap.put(sequenceIdName, newLastId);
      return true;
    }

    @Override
    public Long getLastId(String sequenceIdName) {
      return sequenceIdToLastIdMap.get(sequenceIdName);
    }

    /**
     * Builder for Ratis based StateManager.
     */
    public static class Builder {
      private Table<String, Long> table;
      private DBTransactionBuffer buffer;
      private SCMRatisServer ratisServer;

      public Builder setRatisServer(final SCMRatisServer scmRatisServer) {
        this.ratisServer = scmRatisServer;
        return this;
      }

      public Builder setSequenceIdTable(
          final Table<String, Long> sequenceIdTable) {
        table = sequenceIdTable;
        return this;
      }

      public Builder setDBTransactionBuffer(DBTransactionBuffer trxBuffer) {
        buffer = trxBuffer;
        return this;
      }

      public StateManager build() {
        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(buffer);
        Preconditions.checkNotNull(ratisServer);

        final StateManager impl = new StateManagerHAImpl(table, buffer);
        final SCMHAInvocationHandler invocationHandler
            = new SCMHAInvocationHandler(SEQUENCE_ID, impl, ratisServer);

        return (StateManager) Proxy.newProxyInstance(
            SCMHAInvocationHandler.class.getClassLoader(),
            new Class<?>[]{StateManager.class},
            invocationHandler);
      }
    }
  }

  /**
   * Default StateManager, writes directly go to RocksDB.
   */
  static final class StateManagerImpl implements StateManager {
    private final Table<String, Long> sequenceIdTable;

    StateManagerImpl(Table<String, Long> sequenceIdTable) {
      this.sequenceIdTable = sequenceIdTable;
      LOG.info("Init default SequenceIdGenerator.");
    }

    @Override
    public Boolean allocateBatch(String sequenceIdName,
                                 Long expectedLastId, Long newLastId) {
      Long lastId;
      try {
        lastId = sequenceIdTable.get(sequenceIdName);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to get lastId from db", ioe);
      }
      if (lastId == null) {
        lastId = INVALID_SEQUENCE_ID;
      }

      if (!lastId.equals(expectedLastId)) {
        LOG.warn("Failed to allocate a batch for {}, expected lastId is {}," +
            " actual lastId is {}.", sequenceIdName, expectedLastId, lastId);
        return false;
      }

      try {
        sequenceIdTable.put(sequenceIdName, newLastId);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to put lastId to db", ioe);
      }
      return true;
    }

    @Override
    public Long getLastId(String sequenceIdName) {
      try {
        return sequenceIdTable.get(sequenceIdName);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to get lastId from db", ioe);
      }
    }
  }
}
