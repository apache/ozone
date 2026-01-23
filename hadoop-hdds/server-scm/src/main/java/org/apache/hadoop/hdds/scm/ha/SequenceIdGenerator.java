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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.SEQUENCE_ID;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SEQUENCE_ID_BATCH_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SEQUENCE_ID_BATCH_SIZE_DEFAULT;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /**
   * Ids supported.
   */
  public static final String LOCAL_ID = "localId";
  public static final String DEL_TXN_ID = "delTxnId";
  public static final String CONTAINER_ID = "containerId";

  // Certificate ID for all services, including root certificates, whose ID
  // were using "rootCertificateId" before.
  public static final String CERTIFICATE_ID = "CertificateId";
  @Deprecated
  public static final String ROOT_CERTIFICATE_ID = "rootCertificateId";

  private static final long INVALID_SEQUENCE_ID = 0;

  private final Map<String, Batch> sequenceIdToBatchMap;

  private final Lock lock;
  private final long batchSize;
  private final StateManager stateManager;

  /**
   * @param conf            : conf
   * @param scmhaManager    : scmhaManager
   * @param sequenceIdTable : sequenceIdTable
   */
  public SequenceIdGenerator(ConfigurationSource conf,
      SCMHAManager scmhaManager, Table<String, Long> sequenceIdTable) {
    this.sequenceIdToBatchMap = new HashMap<>();
    this.lock = new ReentrantLock();
    this.batchSize = conf.getInt(OZONE_SCM_SEQUENCE_ID_BATCH_SIZE,
        OZONE_SCM_SEQUENCE_ID_BATCH_SIZE_DEFAULT);

    Objects.requireNonNull(scmhaManager, "scmhaManager == null");
    this.stateManager = createStateManager(scmhaManager, sequenceIdTable);
  }

  public StateManager createStateManager(SCMHAManager scmhaManager,
      Table<String, Long> sequenceIdTable) {
    Objects.requireNonNull(scmhaManager, "scmhaManager == null");
    return new StateManagerImpl.Builder()
        .setRatisServer(scmhaManager.getRatisServer())
        .setDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .setSequenceIdTable(sequenceIdTable).build();
  }

  /**
   * @param sequenceIdName : name of the sequenceId
   * @return : next id of this sequenceId.
   */
  public long getNextId(String sequenceIdName) throws SCMException {
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
        long nextLastId = batch.lastId +
            ((sequenceIdName.equals(CERTIFICATE_ID)) ? 1 : batchSize);

        if (stateManager.allocateBatch(sequenceIdName,
            prevLastId, nextLastId)) {
          batch.lastId = nextLastId;
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
      invalidateBatchInternal();
    } finally {
      lock.unlock();
    }
  }

  private void invalidateBatchInternal() {
    sequenceIdToBatchMap
        .forEach((sequenceId, batch) -> batch.nextId = batch.lastId + 1);
  }

  /**
   * Reinitialize the SequenceIdGenerator with the latest sequenceIdTable
   * during SCM reload.
   */
  public void reinitialize(Table<String, Long> sequenceIdTable)
      throws IOException {
    LOG.info("reinitialize SequenceIdGenerator.");
    lock.lock();
    try {
      invalidateBatchInternal();
      stateManager.reinitialize(sequenceIdTable);
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
                          Long expectedLastId, Long newLastId)
        throws SCMException;

    /**
     * @param sequenceIdName : name of the sequence id.
     * @return lastId saved in db
     */
    Long getLastId(String sequenceIdName);

    /**
     * Reinitialize the SequenceIdGenerator with the latest sequenceIdTable
     * during SCM reload.
     */
    void reinitialize(Table<String, Long> sequenceIdTable) throws IOException;
  }

  /**
   * Ratis based StateManager, db operations are queued in
   * DBTransactionBuffer until a snapshot is taken.
   */
  static final class StateManagerImpl implements StateManager {
    private Table<String, Long> sequenceIdTable;
    private final DBTransactionBuffer transactionBuffer;
    private final Map<String, Long> sequenceIdToLastIdMap;

    private StateManagerImpl(Table<String, Long> sequenceIdTable,
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
        transactionBuffer
            .addToBuffer(sequenceIdTable, sequenceIdName, newLastId);
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

    @Override
    public void reinitialize(Table<String, Long> seqIdTable)
        throws IOException {
      this.sequenceIdTable = seqIdTable;
      this.sequenceIdToLastIdMap.clear();
      initialize();
    }

    private void initialize() throws IOException {
      try (Table.KeyValueIterator<String, Long> iterator = sequenceIdTable.iterator()) {

        while (iterator.hasNext()) {
          Table.KeyValue<String, Long> kv = iterator.next();
          final String sequenceIdName = kv.getKey();
          final Long lastId = kv.getValue();
          Objects.requireNonNull(sequenceIdName,
              "sequenceIdName should not be null");
          Objects.requireNonNull(lastId,
              "lastId should not be null");
          sequenceIdToLastIdMap.put(sequenceIdName, lastId);
        }
      }
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
        Objects.requireNonNull(table, "table == null");
        Objects.requireNonNull(buffer, "buffer == null");

        final StateManager impl = new StateManagerImpl(table, buffer);

        return ratisServer.getProxyHandler(SEQUENCE_ID, StateManager.class, impl);
      }
    }
  }

  /**
   * TODO
   *  Relocate the code after upgrade framework is ready.
   *
   * Upgrade localID, delTxnId, containerId from legacy solution
   * to SequenceIdGenerator.
   */
  public static void upgradeToSequenceId(SCMMetadataStore scmMetadataStore)
      throws IOException {
    Table<String, Long> sequenceIdTable = scmMetadataStore.getSequenceIdTable();

    // upgrade localId
    // Short-term solution: when setup multi SCM from scratch, they need
    // achieve an agreement upon the initial value of LOCAL_ID.
    // Long-term solution: the bootstrapped SCM will explicitly download
    // scm.db from leader SCM, and drop its own scm.db. Thus the upgrade
    // operations can take effect exactly once in a SCM HA cluster.
    if (sequenceIdTable.get(LOCAL_ID) == null) {
      long millisSinceEpoch = TimeUnit.DAYS.toMillis(
          LocalDate.of(LocalDate.now().getYear() + 1, 1, 1).toEpochDay());

      long localId = millisSinceEpoch << Short.SIZE;
      Preconditions.checkArgument(localId > UniqueId.next());

      sequenceIdTable.put(LOCAL_ID, localId);
      LOG.info("upgrade {} to {}", LOCAL_ID, sequenceIdTable.get(LOCAL_ID));
    }

    // upgrade delTxnId
    if (sequenceIdTable.get(DEL_TXN_ID) == null) {
      // fetch delTxnId from DeletedBlocksTXTable
      // check HDDS-4477 for details.
      DeletedBlocksTransaction txn
          = scmMetadataStore.getDeletedBlocksTXTable().get(0L);
      sequenceIdTable.put(DEL_TXN_ID, txn != null ? txn.getTxID() : 0L);
      LOG.info("upgrade {} to {}", DEL_TXN_ID, sequenceIdTable.get(DEL_TXN_ID));
    }

    // upgrade containerId
    if (sequenceIdTable.get(CONTAINER_ID) == null) {
      long largestContainerId = 0;
      try (TableIterator<ContainerID, ContainerInfo> iterator
          = scmMetadataStore.getContainerTable().valueIterator()) {
        while (iterator.hasNext()) {
          final ContainerInfo containerInfo = iterator.next();
          largestContainerId =
              Long.max(containerInfo.getContainerID(), largestContainerId);
        }
      }

      sequenceIdTable.put(CONTAINER_ID, largestContainerId);
      LOG.info("upgrade {} to {}",
          CONTAINER_ID, sequenceIdTable.get(CONTAINER_ID));
    }

    upgradeToCertificateSequenceId(scmMetadataStore, false);
  }

  public static void upgradeToCertificateSequenceId(
      SCMMetadataStore scmMetadataStore, boolean force) throws IOException {
    Table<String, Long> sequenceIdTable = scmMetadataStore.getSequenceIdTable();

    // upgrade certificate ID table
    if (sequenceIdTable.get(CERTIFICATE_ID) == null || force) {
      // Start from ID 2.
      // ID 1 - root certificate, ID 2 - first SCM certificate.
      long largestCertId = BigInteger.ONE.add(BigInteger.ONE).longValueExact();
      try (TableIterator<BigInteger, X509Certificate> iterator
          = scmMetadataStore.getValidSCMCertsTable().valueIterator()) {
        while (iterator.hasNext()) {
          final X509Certificate cert = iterator.next();
          largestCertId = Long.max(cert.getSerialNumber().longValueExact(),
              largestCertId);
        }
      }

      try (TableIterator<BigInteger, X509Certificate> iterator
          = scmMetadataStore.getValidCertsTable().valueIterator()) {
        while (iterator.hasNext()) {
          final X509Certificate cert = iterator.next();
          largestCertId = Long.max(
              cert.getSerialNumber().longValueExact(), largestCertId);
        }
      }

      sequenceIdTable.put(CERTIFICATE_ID, largestCertId);
      LOG.info("upgrade {} to {}", CERTIFICATE_ID,
          sequenceIdTable.get(CERTIFICATE_ID));
    }

    // delete the ROOT_CERTIFICATE_ID record if exists
    // ROOT_CERTIFICATE_ID is replaced with CERTIFICATE_ID now
    if (sequenceIdTable.get(ROOT_CERTIFICATE_ID) != null) {
      sequenceIdTable.delete(ROOT_CERTIFICATE_ID);
    }
  }

  static class Batch {
    // The upper bound of the batch.
    private long lastId = INVALID_SEQUENCE_ID;
    // The next id to be allocated in this batch.
    private long nextId = lastId + 1;
  }
}
