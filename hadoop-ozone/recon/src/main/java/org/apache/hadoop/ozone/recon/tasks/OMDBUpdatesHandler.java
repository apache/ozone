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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to listen on OM RocksDB updates.
 */
public class OMDBUpdatesHandler extends ManagedWriteBatch.Handler {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBUpdatesHandler.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private Map<Integer, String> tablesNames;
  private OMMetadataManager omMetadataManager;
  private List<OMDBUpdateEvent> omdbUpdateEvents = new ArrayList<>();
  private Map<String, Map<Object, OMDBUpdateEvent>> omdbLatestUpdateEvents = new HashMap<>();
  private final OMDBDefinition omdbDefinition = OMDBDefinition.get();
  private final OmUpdateEventValidator omUpdateEventValidator = new OmUpdateEventValidator(omdbDefinition);
  private long batchSequenceNumber; // Store the current sequence number for the batch

  public OMDBUpdatesHandler(OMMetadataManager metadataManager) {
    omMetadataManager = metadataManager;
    tablesNames = metadataManager.getStore().getTableNames();
  }

  public void setLatestSequenceNumber(long sequenceNumber) {
    this.batchSequenceNumber = sequenceNumber;
  }

  public long getLatestSequenceNumber() {
    return this.batchSequenceNumber;
  }

  @Override
  public void put(int cfIndex, byte[] keyBytes, byte[] valueBytes) {
    try {
      processEvent(cfIndex, keyBytes, valueBytes,
          OMDBUpdateEvent.OMDBUpdateAction.PUT);
    } catch (IOException ioEx) {
      LOG.error("Exception when reading key : ", ioEx);
    }
  }

  @Override
  public void delete(int cfIndex, byte[] keyBytes) {
    try {
      processEvent(cfIndex, keyBytes, null,
          OMDBUpdateEvent.OMDBUpdateAction.DELETE);
    } catch (IOException ioEx) {
      LOG.error("Exception when reading key : ", ioEx);
    }
  }

  /**
   * Processes an OM DB update event based on the provided parameters.
   *
   * @param cfIndex     Index of the column family.
   * @param keyBytes    Serialized key bytes.
   * @param valueBytes  Serialized value bytes.
   * @param action      Type of the database action (e.g., PUT, DELETE).
   * @throws IOException If an I/O error occurs.
   */
  private void processEvent(int cfIndex, byte[] keyBytes, byte[]
      valueBytes, OMDBUpdateEvent.OMDBUpdateAction action)
      throws IOException {

    if (closed.get()) {
      throw new IllegalStateException("OMDBUpdatesHandler has been closed");
    }

    String tableName = tablesNames.get(cfIndex);
    // DTOKEN_TABLE is using OzoneTokenIdentifier as key instead of String
    // and assuming to typecast as String while de-serializing will throw error.
    // omdbLatestUpdateEvents defines map key as String type to store in its map
    // and to change to Object as key will have larger impact considering all
    // ReconOmTasks. Currently, this table is not needed to sync in Recon OM DB
    // snapshot as this table data not being used currently in Recon.
    // When this table data will be needed, all events for this table will be
    // saved using Object as key and new task will also retrieve using Object
    // as key.
    final DBColumnFamilyDefinition<?, ?> cf
        = omdbDefinition.getColumnFamily(tableName);
    if (cf != null) {
      OMDBUpdateEvent.OMUpdateEventBuilder builder =
          new OMDBUpdateEvent.OMUpdateEventBuilder<>();
      builder.setTable(tableName);
      builder.setAction(action);
      final Object key = cf.getKeyCodec().fromPersistedFormat(keyBytes);
      builder.setKey(key);

      // Initialize table-specific event map if it does not exist
      omdbLatestUpdateEvents.putIfAbsent(tableName, new HashMap<>());
      Map<Object, OMDBUpdateEvent> tableEventsMap = omdbLatestUpdateEvents.get(tableName);

      // Handle the event based on its type:
      // - PUT with a new key: Insert the new value.
      // - PUT with an existing key: Update the existing value.
      // - DELETE with an existing key: Remove the value.
      // - DELETE with a non-existing key: No action, log a warning if
      // necessary.
      Table table = omMetadataManager.getTable(tableName);

      OMDBUpdateEvent latestEvent = tableEventsMap.get(key);
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
        if (!omUpdateEventValidator.isValidEvent(tableName, value, key,
            action)) {
          return;
        }

        builder.setValue(value);
        // Tag PUT operations on existing keys as "UPDATE" events.
        if (oldValue != null) {

          // If the oldValue is not valid for this event, we skip it.
          if (!omUpdateEventValidator.isValidEvent(tableName, oldValue, key,
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
          String keyStr = (key instanceof String) ? key.toString() : "";
          if (keyStr.isEmpty()) {
            LOG.warn(
                "Only DTOKEN_TABLE table uses OzoneTokenIdentifier as key " +
                    "instead of String. Event on any other table in this " +
                    "condition may need to be investigated. This DELETE " +
                    "event is on {} table which is not useful for Recon to " +
                    "capture.", tableName);
          }
          LOG.debug("Old Value of Key: {} in table: {} should not be null " +
              "for DELETE event ", keyStr, tableName);
          return;
        }
        if (!omUpdateEventValidator.isValidEvent(tableName, oldValue, key,
            action)) {
          return;
        }
        // When you delete a Key, we add the old value to the event so that
        // a downstream task can use it.
        builder.setValue(oldValue);
      }

      OMDBUpdateEvent event = builder.build();
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Generated OM update Event for table : %s, " +
                "action = %s", tableName, action));
      }
      omdbUpdateEvents.add(event);
      tableEventsMap.put(key, event);
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

  @Override
  public void markCommitWithTimestamp(final byte[] xid, final byte[] ts)
      throws RocksDBException {
    /**
     * There are no use cases yet for this method in Recon. These will be
     * implemented as and when need arises.
     */

  }

  @Override
  public void close() {
    super.close();
    if (closed.compareAndSet(false, true)) {
      LOG.debug("Closing OMDBUpdatesHandler");

      // Clear internal tracking map to help GC
      // Note: We do NOT close tables obtained from omMetadataManager as they
      // are owned and managed by the OMMetadataManager, not by this handler.
      // Note: omdbUpdateEvents is intentionally NOT cleared here because
      // getEvents() may be called after close() to retrieve the events
      // for processing by ReconOmTasks
      if (omdbLatestUpdateEvents != null) {
        omdbLatestUpdateEvents.clear();
      }

      LOG.debug("OMDBUpdatesHandler cleanup completed");
    }
  }

  /**
   * Get List of events.
   * @return List of events.
   */
  public List<OMDBUpdateEvent> getEvents() {
    return omdbUpdateEvents;
  }
}
