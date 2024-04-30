/*
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

package org.apache.hadoop.ozone.recon.scm;

import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.ozone.recon.tasks.RocksDBDBUpdateEventValidator;
import org.apache.hadoop.ozone.recon.tasks.RocksDBUpdateEvent;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.recon.tasks.RocksDBUpdateEvent.RocksDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.RocksDBUpdateEvent.RocksDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.RocksDBUpdateEvent.RocksDBUpdateAction.UPDATE;

/**
 * Class used to listen on SCM RocksDB updates.
 */
public class SCMDBUpdatesHandler extends ManagedWriteBatch.Handler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDBUpdatesHandler.class);

  private Map<Integer, String> tablesNames;
  private ReconScmMetadataManager reconScmMetadataManager;
  private List<RocksDBUpdateEvent> scmDBUpdateEvents = new ArrayList<>();
  private Map<Object, RocksDBUpdateEvent> scmDBLatestUpdateEventsMap = new HashMap<>();
  private ReconSCMDBDefinition reconSCMDBDefinition;
  private RocksDBDBUpdateEventValidator scmDBUpdateEventValidator;

  public SCMDBUpdatesHandler(ReconScmMetadataManager reconScmMetadataManager) {
    this.reconScmMetadataManager = reconScmMetadataManager;
    tablesNames = reconScmMetadataManager.getStore().getTableNames();
    reconSCMDBDefinition = new ReconSCMDBDefinition();
    scmDBUpdateEventValidator = new RocksDBDBUpdateEventValidator(reconSCMDBDefinition);
  }

  @Override
  public void put(int cfIndex, byte[] keyBytes, byte[] valueBytes) {
    try {
      processEvent(cfIndex, keyBytes, valueBytes,
          RocksDBUpdateEvent.RocksDBUpdateAction.PUT);
    } catch (IOException ioEx) {
      LOG.error("Exception when reading key : ", ioEx);
    }
  }

  @Override
  public void delete(int cfIndex, byte[] keyBytes) {
    try {
      processEvent(cfIndex, keyBytes, null,
          RocksDBUpdateEvent.RocksDBUpdateAction.DELETE);
    } catch (IOException ioEx) {
      LOG.error("Exception when reading key : ", ioEx);
    }
  }

  /**
   * Processes an OM or SCM DB update event based on the provided parameters.
   *
   * @param cfIndex     Index of the column family.
   * @param keyBytes    Serialized key bytes.
   * @param valueBytes  Serialized value bytes.
   * @param action      Type of the database action (e.g., PUT, DELETE).
   * @throws IOException If an I/O error occurs.
   */
  private void processEvent(int cfIndex, byte[] keyBytes, byte[]
      valueBytes, RocksDBUpdateEvent.RocksDBUpdateAction action)
      throws IOException {
    String tableName = tablesNames.get(cfIndex);
    final DBColumnFamilyDefinition<?, ?> cf
        = reconSCMDBDefinition.getColumnFamily(tableName);
    if (cf != null) {
      RocksDBUpdateEvent.RocksDBUpdateEventBuilder builder =
          new RocksDBUpdateEvent.RocksDBUpdateEventBuilder<>();
      builder.setTable(tableName);
      builder.setAction(action);
      final Object key = cf.getKeyCodec().fromPersistedFormat(keyBytes);
      builder.setKey(key);

      // Handle the event based on its type:
      // - PUT with a new key: Insert the new value.
      // - PUT with an existing key: Update the existing value.
      // - DELETE with an existing key: Remove the value.
      // - DELETE with a non-existing key: No action, log a warning if
      // necessary.
      Table table = reconScmMetadataManager.getTable(tableName);

      RocksDBUpdateEvent latestEvent = scmDBLatestUpdateEventsMap.get(key);
      Object oldValue;
      if (latestEvent != null) {
        oldValue = latestEvent.getValue();
      } else {
        // Recon does not add entries to cache, and it is safer to always use
        // getSkipCache in Recon.
        oldValue = table.getSkipCache(key);
      }

      if (action.equals(PUT)) {
        final Object value = cf.getValueCodec().fromPersistedFormat(valueBytes);

        // If the updated value is not valid for this event, we skip it.
        if (!scmDBUpdateEventValidator.isValidEvent(tableName, value, key,
            action)) {
          return;
        }

        builder.setValue(value);
        // Tag PUT operations on existing keys as "UPDATE" events.
        if (oldValue != null) {

          // If the oldValue is not valid for this event, we skip it.
          if (!scmDBUpdateEventValidator.isValidEvent(tableName, oldValue, key,
              action)) {
            return;
          }

          builder.setOldValue(oldValue);
          if (latestEvent == null || latestEvent.getAction() != DELETE) {
            builder.setAction(UPDATE);
          }
        }
      } else if (action.equals(DELETE)) {
        if (null == oldValue) {
          LOG.warn("Old Value of Key: {} in table: {} should not be null " +
              "for DELETE event ", key.toString(), tableName);
          return;
        }
        if (!scmDBUpdateEventValidator.isValidEvent(tableName, oldValue, key,
            action)) {
          return;
        }
        // When you delete a Key, we add the old value to the event so that
        // a downstream task can use it.
        builder.setValue(oldValue);
      }

      RocksDBUpdateEvent event = builder.build();
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Generated SCM update Event for table : %s, action = %s", tableName, action));
      }
      scmDBUpdateEvents.add(event);
      scmDBLatestUpdateEventsMap.put(key, event);
    } else {
      // Log and ignore events if key or value types are undetermined.
      if (LOG.isWarnEnabled()) {
        LOG.warn(String.format("KeyType or ValueType could not be determined" +
            " for table %s. Ignoring the event.", tableName));
      }
    }
  }

  @Override
  public void put(byte[] bytes, byte[] bytes1) {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void merge(int i, byte[] bytes, byte[] bytes1)
      throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void merge(byte[] bytes, byte[] bytes1) {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void delete(byte[] bytes) {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void singleDelete(int i, byte[] bytes) throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void singleDelete(byte[] bytes) {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void deleteRange(int i, byte[] bytes, byte[] bytes1)
      throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void deleteRange(byte[] bytes, byte[] bytes1) {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void logData(byte[] bytes) {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void putBlobIndex(int i, byte[] bytes, byte[] bytes1)
      throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void markBeginPrepare() throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void markEndPrepare(byte[] bytes) throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void markNoop(boolean b) throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void markRollback(byte[] bytes) throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  @Override
  public void markCommit(byte[] bytes) throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */
  }

  public void markCommitWithTimestamp(final byte[] xid, final byte[] ts)
      throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */

  }

  /**
   * Get List of events.
   * @return List of events.
   */
  public List<RocksDBUpdateEvent> getEvents() {
    return scmDBUpdateEvents;
  }
}
