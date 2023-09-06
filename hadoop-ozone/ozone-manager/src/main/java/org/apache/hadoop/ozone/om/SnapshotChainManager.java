/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is used for creating and accessing Snapshot Chains.
 * <p>
 * The snapshot chain maintains the in-memory sequence of snapshots
 * created in chronological order.  There are two such snapshots maintained
 * i.) Path based snapshot chain, sequence of snapshots created for a
 * given /volume/bucket
 * ii.) Global snapshot chain, sequence of all snapshots created in order
 * <p>
 * On start, the snapshot chains are initialized from the on disk
 * SnapshotInfoTable from the om RocksDB.
 */
public class SnapshotChainManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotChainManager.class);

  private final Map<UUID, SnapshotChainInfo> globalSnapshotChain;
  private final ConcurrentMap<String, LinkedHashMap<UUID, SnapshotChainInfo>>
      snapshotChainByPath;
  private final ConcurrentMap<String, UUID> latestSnapshotIdByPath;
  private final ConcurrentMap<UUID, String> snapshotIdToTableKey;
  private UUID latestGlobalSnapshotId;

  public SnapshotChainManager(OMMetadataManager metadataManager) {
    globalSnapshotChain = Collections.synchronizedMap(new LinkedHashMap<>());
    snapshotChainByPath = new ConcurrentHashMap<>();
    latestSnapshotIdByPath = new ConcurrentHashMap<>();
    snapshotIdToTableKey = new ConcurrentHashMap<>();
    latestGlobalSnapshotId = null;
    loadFromSnapshotInfoTable(metadataManager);
  }

  /**
   * Add snapshot to global snapshot chain.
   */
  private void addSnapshotGlobal(UUID snapshotID, UUID prevGlobalID) {
    if (globalSnapshotChain.containsKey(snapshotID)) {
      throw new IllegalStateException(String.format(
          "Global Snapshot chain corruption. Snapshot with snapshotId: %s is " +
              "already present in the chain.", snapshotID));
    }
    if (globalSnapshotChain.size() > 0 && prevGlobalID == null) {
      throw new IllegalStateException(String.format("Snapshot chain " +
          "corruption. Adding snapshot %s as head node while there are %d " +
              "snapshots in the global snapshot chain.", snapshotID,
          globalSnapshotChain.size()));
    }

    if (prevGlobalID != null &&
        !globalSnapshotChain.containsKey(prevGlobalID)) {
      throw new IllegalStateException(String.format(
          "Global Snapshot chain corruption. Previous snapshotId: %s is " +
              "set for snapshotId: %s but no associated snapshot found in " +
              "snapshot chain.", prevGlobalID, snapshotID));
    }

    if (prevGlobalID != null) {
      if (globalSnapshotChain.get(prevGlobalID).hasNextSnapshotId()) {
        throw new IllegalStateException(String.format(
            "Global Snapshot chain corruption. Snapshot with snapshotId: %s " +
                "already has the next snapshotId: %s. Adding snapshot %s " +
                "with prevSnapshotId: %s will make the chain non linear.",
            prevGlobalID,
            globalSnapshotChain.get(prevGlobalID).getNextSnapshotId(),
            snapshotID, prevGlobalID));
      }
      // On add snapshot, set previous snapshot entry nextSnapshotID =
      // snapshotID
      globalSnapshotChain.get(prevGlobalID).setNextSnapshotId(snapshotID);
    }

    globalSnapshotChain.put(snapshotID,
        new SnapshotChainInfo(snapshotID, prevGlobalID, null));

    // set state variable latestGlobal snapshot entry to this snapshotID
    latestGlobalSnapshotId = snapshotID;
  }

  /**
   * Add snapshot to bucket snapshot chain(path based).
   */
  private void addSnapshotPath(String snapshotPath, UUID snapshotID,
                               UUID prevPathID) {
    // On add snapshot, set previous snapshot entry nextSnapshotId = snapshotId
    if (prevPathID != null &&
        ((!snapshotChainByPath.containsKey(snapshotPath)) ||
            (!snapshotChainByPath.get(snapshotPath).containsKey(prevPathID)))) {
      throw new IllegalStateException(String.format(
          "Path Snapshot chain corruption. Previous snapshotId: %s is set " +
              "for snapshotId: %s but no associated snapshot found in " +
              "snapshot chain.", prevPathID, snapshotID));
    }

    if (prevPathID == null && snapshotChainByPath.containsKey(snapshotPath) &&
        !snapshotChainByPath.get(snapshotPath).isEmpty()) {
      throw new IllegalStateException(String.format(
          "Path Snapshot chain corruption. Error while adding snapshot with " +
              "snapshotId %s with as the first snapshot in snapshot path: " +
              "%s which already has %d snapshots.", snapshotID, snapshotPath,
          snapshotChainByPath.get(snapshotPath).size()));
    }

    if (prevPathID != null && snapshotChainByPath.containsKey(snapshotPath)) {
      if (snapshotChainByPath.get(snapshotPath).get(prevPathID)
          .hasNextSnapshotId()) {
        throw new IllegalStateException(String.format(
            "Path Snapshot chain corruption. Next snapshotId: %s is already " +
                "set for snapshotId: %s. Adding snapshotId: %s with " +
                "prevSnapshotId: %s will make the chain non linear.",
            snapshotChainByPath.get(snapshotPath).get(prevPathID)
                .getNextSnapshotId(), prevPathID,
            snapshotID, prevPathID));
      }
      snapshotChainByPath
          .get(snapshotPath)
          .get(prevPathID)
          .setNextSnapshotId(snapshotID);
    }

    if (!snapshotChainByPath.containsKey(snapshotPath)) {
      snapshotChainByPath.put(snapshotPath, new LinkedHashMap<>());
    }

    snapshotChainByPath.get(snapshotPath)
        .put(snapshotID, new SnapshotChainInfo(snapshotID, prevPathID, null));

    // set state variable latestPath snapshot entry to this snapshotID
    latestSnapshotIdByPath.put(snapshotPath, snapshotID);
  }

  private boolean deleteSnapshotGlobal(UUID snapshotID) throws IOException {
    if (globalSnapshotChain.containsKey(snapshotID)) {
      // reset prev and next snapshot entries in chain ordered list
      // for node removal
      UUID next = globalSnapshotChain.get(snapshotID).getNextSnapshotId();
      UUID prev = globalSnapshotChain.get(snapshotID).getPreviousSnapshotId();

      if (prev != null && !globalSnapshotChain.containsKey(prev)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: %s to be deleted has previous snapshotId: %s " +
                "but associated snapshot is not found in snapshot chain.",
            snapshotID, prev));
      }
      if (next != null && !globalSnapshotChain.containsKey(next)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: {%s} to be deleted has next snapshotId: %s " +
                "but associated snapshot is not found in snapshot chain.",
            snapshotID, next));
      }
      globalSnapshotChain.remove(snapshotID);
      if (next != null) {
        globalSnapshotChain.get(next).setPreviousSnapshotId(prev);
      }
      if (prev != null) {
        globalSnapshotChain.get(prev).setNextSnapshotId(next);
      }
      // remove from latest list if necessary
      if (latestGlobalSnapshotId.equals(snapshotID)) {
        latestGlobalSnapshotId = prev;
      }
      return true;
    } else {
      // snapshotID not found in snapshot chain, log warning and return
      LOG.warn("Snapshot chain corruption. SnapshotID: {} is not found in " +
          "snapshot chain.", snapshotID);
      return false;
    }
  }

  private boolean deleteSnapshotPath(String snapshotPath,
                                     UUID snapshotId) throws IOException {
    if (snapshotChainByPath.containsKey(snapshotPath) &&
        snapshotChainByPath.get(snapshotPath).containsKey(snapshotId)) {
      // reset prev and next snapshot entries in chain ordered list
      // for node removal
      UUID nextSnapshotId = snapshotChainByPath
          .get(snapshotPath)
          .get(snapshotId)
          .getNextSnapshotId();
      UUID previousSnapshotId = snapshotChainByPath
          .get(snapshotPath)
          .get(snapshotId)
          .getPreviousSnapshotId();

      if (previousSnapshotId != null &&
          !snapshotChainByPath.get(snapshotPath)
              .containsKey(previousSnapshotId)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: %s at snapshotPath: %s to be deleted has " +
                "previous snapshotId: %s but associated snapshot is not " +
                "found in snapshot chain.", snapshotId, snapshotPath,
            previousSnapshotId));
      }
      if (nextSnapshotId != null && !snapshotChainByPath.get(snapshotPath)
          .containsKey(nextSnapshotId)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: %s at snapshotPath: %s to be deleted has next " +
                "snapshotId: %s but associated snapshot is not found in " +
                "snapshot chain.", snapshotId, snapshotPath,
            nextSnapshotId));
      }

      snapshotChainByPath.get(snapshotPath).remove(snapshotId);
      if (nextSnapshotId != null) {
        snapshotChainByPath.get(snapshotPath)
            .get(nextSnapshotId)
            .setPreviousSnapshotId(previousSnapshotId);
      }
      if (previousSnapshotId != null) {
        snapshotChainByPath.get(snapshotPath)
            .get(previousSnapshotId)
            .setNextSnapshotId(nextSnapshotId);
      }
      // remove path if no entries
      if (snapshotChainByPath.get(snapshotPath).isEmpty()) {
        snapshotChainByPath.remove(snapshotPath);
      }
      // remove from latest list if necessary
      if (latestSnapshotIdByPath.get(snapshotPath).equals(snapshotId)) {
        latestSnapshotIdByPath.remove(snapshotPath);
        if (previousSnapshotId != null) {
          latestSnapshotIdByPath.put(snapshotPath, previousSnapshotId);
        }
      }
      return true;
    } else {
      // snapshotId not found in snapshot chain, log warning and return
      LOG.warn("Snapshot chain corruption. SnapshotId: {} is not in chain " +
          "found for snapshot path {}.", snapshotId, snapshotPath);
      return false;
    }
  }

  /**
   * Loads the snapshot chain from SnapshotInfo table.
   */
  private void loadFromSnapshotInfoTable(OMMetadataManager metadataManager) {
    // read from snapshotInfo table to populate
    // snapshot chains - both global and local path
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
             keyIter = metadataManager.getSnapshotInfoTable().iterator()) {
      Map<UUID, SnapshotInfo> snaps = new HashMap<>();
      // Forward Linked list for snapshot chain.
      Map<UUID, UUID> snapshotToNextSnapshotMap = new HashMap<>();
      UUID head = null;
      Table.KeyValue<String, SnapshotInfo> kv;
      globalSnapshotChain.clear();
      snapshotChainByPath.clear();
      latestSnapshotIdByPath.clear();
      snapshotIdToTableKey.clear();

      while (keyIter.hasNext()) {
        kv = keyIter.next();
        SnapshotInfo snapshotInfo = kv.getValue();
        snaps.put(kv.getValue().getSnapshotId(), snapshotInfo);
        if (snapshotInfo.getGlobalPreviousSnapshotId() != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Global Snapshot chain link {} -> {}",
                snapshotInfo.getGlobalPreviousSnapshotId(),
                snapshotInfo.getSnapshotId());
          }
          // Adding edge to the linked list. prevGlobalSnapId -> snapId
          snapshotToNextSnapshotMap.put(
              snapshotInfo.getGlobalPreviousSnapshotId(),
              snapshotInfo.getSnapshotId());
        } else {
          head = snapshotInfo.getSnapshotId();
        }
      }
      int size = 0;
      UUID prev = null;
      while (head != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding Snapshot Info: {}", snaps.get(head));
        }
        addSnapshot(snaps.get(head));
        size += 1;
        prev = head;
        head = snapshotToNextSnapshotMap.get(head);
      }
      if (size != snaps.size()) {
        throw new IllegalStateException(String.format("Snapshot chain " +
            "corruption. All snapshots have not been added to the " +
            "snapshot chain. Last snapshot added to chain : %s", prev));
      }
    } catch (IOException ioException) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(ioException);
    }
  }

  /**
   * Add snapshot to snapshot chain.
   */
  public synchronized void addSnapshot(SnapshotInfo snapshotInfo) {
    addSnapshotGlobal(snapshotInfo.getSnapshotId(),
        snapshotInfo.getGlobalPreviousSnapshotId());
    addSnapshotPath(snapshotInfo.getSnapshotPath(),
        snapshotInfo.getSnapshotId(),
        snapshotInfo.getPathPreviousSnapshotId());
    // store snapshot ID to snapshot DB table key in the map
    snapshotIdToTableKey.put(snapshotInfo.getSnapshotId(),
        snapshotInfo.getTableKey());
  }

  /**
   * Delete snapshot from snapshot chain.
   */
  public synchronized boolean deleteSnapshot(SnapshotInfo snapshotInfo)
      throws IOException {
    boolean status = deleteSnapshotGlobal(snapshotInfo.getSnapshotId()) &&
        deleteSnapshotPath(snapshotInfo.getSnapshotPath(),
            snapshotInfo.getSnapshotId());
    if (status) {
      snapshotIdToTableKey.remove(snapshotInfo.getSnapshotId());
    }
    return status;
  }

  /**
   * Get latest global snapshot in snapshot chain.
   */
  public UUID getLatestGlobalSnapshotId() {
    return latestGlobalSnapshotId;
  }

  /**
   * Get latest path snapshot in snapshot chain.
   */
  public UUID getLatestPathSnapshotId(String snapshotPath) {
    return latestSnapshotIdByPath.get(snapshotPath);
  }

  /**
   * Returns true if snapshot from given snapshotId has a next snapshot entry
   * in the global snapshot chain.
   */
  public boolean hasNextGlobalSnapshot(UUID snapshotId) {
    if (!globalSnapshotChain.containsKey(snapshotId)) {
      LOG.error("No snapshot for provided snapshotId: {}", snapshotId);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotId));
    }
    return globalSnapshotChain.get(snapshotId).getNextSnapshotId() != null;
  }

  /**
   * Get next global snapshot in snapshot chain from given snapshot.
   */
  public UUID nextGlobalSnapshot(UUID snapshotId) {
    if (!hasNextGlobalSnapshot(snapshotId)) {
      LOG.error("No snapshot for provided snapshotId: {}", snapshotId);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotId));
    }
    return globalSnapshotChain.get(snapshotId).getNextSnapshotId();
  }

  /**
   * Returns true if snapshot from given snapshotId has a previous snapshot
   * entry in the global snapshot chain.
   */
  public boolean hasPreviousGlobalSnapshot(UUID snapshotId) {
    if (!globalSnapshotChain.containsKey(snapshotId)) {
      LOG.error("No snapshot found in snapshot chain for provided " +
          "snapshotId: {}.", snapshotId);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotId));
    }

    return globalSnapshotChain.get(snapshotId).getPreviousSnapshotId() != null;
  }

  /**
   * Get previous global snapshot in snapshot chain from given snapshot.
   */
  public UUID previousGlobalSnapshot(UUID snapshotId) {
    if (!hasPreviousGlobalSnapshot(snapshotId)) {
      LOG.error("No preceding snapshot found in snapshot chain for provided " +
          "snapshotId: {}.", snapshotId);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotId));
    }
    return globalSnapshotChain.get(snapshotId).getPreviousSnapshotId();
  }

  /**
   * Returns true if snapshot path from given snapshotId has a next snapshot
   * entry in the path snapshot chain.
   */
  public boolean hasNextPathSnapshot(String snapshotPath, UUID snapshotId) {
    if (!snapshotChainByPath.containsKey(snapshotPath) ||
        !snapshotChainByPath.get(snapshotPath).containsKey(snapshotId)) {
      LOG.error("No snapshot found for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotId, snapshotPath);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
              "found in snapshot chain for snapshotPath: %s.", snapshotId,
          snapshotPath));
    }

    return snapshotChainByPath
        .get(snapshotPath)
        .get(snapshotId)
        .getNextSnapshotId() != null;
  }

  /**
   * Get next path snapshot in snapshot chain from given snapshot.
   */
  public UUID nextPathSnapshot(String snapshotPath, UUID snapshotId) {
    if (!hasNextPathSnapshot(snapshotPath, snapshotId)) {
      LOG.error("No following snapshot for provided snapshotId {} and " +
          "snapshotPath {}.", snapshotId, snapshotPath);
      throw new NoSuchElementException(String.format("No following snapshot " +
          "found in snapshot chain for snapshotId: %s and snapshotPath: " +
          "%s.", snapshotId, snapshotPath));
    }
    return snapshotChainByPath.get(snapshotPath)
        .get(snapshotId)
        .getNextSnapshotId();
  }

  /**
   * Returns true if snapshot path from given snapshotId has a
   * previous snapshot entry in the path snapshot chain.
   */
  public boolean hasPreviousPathSnapshot(String snapshotPath,
                                         UUID snapshotId) {
    if (!snapshotChainByPath.containsKey(snapshotPath) ||
        !snapshotChainByPath.get(snapshotPath).containsKey(snapshotId)) {
      LOG.error("No snapshot found for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotId, snapshotPath);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
              "found in snapshot chain for snapshotPath: %s.", snapshotId,
          snapshotPath));
    }
    return snapshotChainByPath.get(snapshotPath)
        .get(snapshotId)
        .getPreviousSnapshotId() != null;
  }

  /**
   * Get previous path snapshot in snapshot chain from given snapshot.
   */
  public UUID previousPathSnapshot(String snapshotPath,
                                   UUID snapshotId) {
    if (!hasPreviousPathSnapshot(snapshotPath, snapshotId)) {
      LOG.error("No preceding snapshot for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotId, snapshotPath);
      throw new NoSuchElementException(String.format("No preceding snapshot " +
          "found in snapshot chain for snapshotId: %s and snapshotPath: " +
          "%s.", snapshotId, snapshotPath));
    }
    return snapshotChainByPath
        .get(snapshotPath)
        .get(snapshotId)
        .getPreviousSnapshotId();
  }

  public String getTableKey(UUID snapshotId) {
    return snapshotIdToTableKey.get(snapshotId);
  }

  public LinkedHashMap<UUID, SnapshotChainInfo> getSnapshotChainPath(
      String path) {
    return snapshotChainByPath.get(path);
  }

  @VisibleForTesting
  public Map<UUID, SnapshotChainInfo> getGlobalSnapshotChain() {
    return globalSnapshotChain;
  }

  @VisibleForTesting
  public Map<String,
      LinkedHashMap<UUID, SnapshotChainInfo>> getSnapshotChainByPath() {
    return snapshotChainByPath;
  }
}
