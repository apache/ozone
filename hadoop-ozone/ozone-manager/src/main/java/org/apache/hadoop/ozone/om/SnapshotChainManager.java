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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final LinkedHashMap<String, SnapshotChainInfo>  snapshotChainGlobal;
  private final Map<String, LinkedHashMap<String, SnapshotChainInfo>>
      snapshotChainPath;
  private String latestGlobalSnapshotID;
  private final Map<String, String> latestPathSnapshotID;
  private final Map<String, String> snapshotIdToTableKey;

  public SnapshotChainManager(OMMetadataManager metadataManager)
      throws IOException {
    snapshotChainGlobal = new LinkedHashMap<>();
    snapshotChainPath = new HashMap<>();
    latestPathSnapshotID = new HashMap<>();
    snapshotIdToTableKey = new HashMap<>();
    latestGlobalSnapshotID = null;
    loadFromSnapshotInfoTable(metadataManager);
  }

  /**
   * Add snapshot to global snapshot chain.
   */
  private void addSnapshotGlobal(String snapshotID,
                                 String prevGlobalID) throws IOException {
    // set previous snapshotID to null if it is "" for
    // internal in-mem structure
    if (prevGlobalID != null && prevGlobalID.isEmpty()) {
      prevGlobalID = null;
    }
    // on add snapshot; set previous snapshot entry nextSnapshotID =
    // snapshotID
    if (prevGlobalID != null &&
        snapshotChainGlobal.containsKey(prevGlobalID)) {
      snapshotChainGlobal
          .get(prevGlobalID)
          .setNextSnapshotID(snapshotID);
    }
    if (prevGlobalID != null &&
        !snapshotChainGlobal.containsKey(prevGlobalID)) {
      throw new IOException(String.format("Snapshot chain corruption. " +
          "Previous snapshotId: %s is set for snapshotId: %s but no " +
          "associated snapshot found in snapshot chain.", prevGlobalID,
          snapshotID));
    }
    snapshotChainGlobal.put(snapshotID,
        new SnapshotChainInfo(snapshotID, prevGlobalID, null));

    // set state variable latestGlobal snapshot entry to this snapshotID
    latestGlobalSnapshotID = snapshotID;
  }

  /**
   * Add snapshot to bucket snapshot chain(path based).
   */
  private void addSnapshotPath(String snapshotPath,
                               String snapshotID,
                               String prevPathID) throws IOException {
    // set previous snapshotID to null if it is "" for
    // internal in-mem structure
    if (prevPathID != null && prevPathID.isEmpty()) {
      prevPathID = null;
    }

    // on add snapshot; set previous snapshot entry nextSnapshotID =
    // snapshotID
    if (prevPathID != null &&
        ((!snapshotChainPath
            .containsKey(snapshotPath)) ||
        (!snapshotChainPath
            .get(snapshotPath)
            .containsKey(prevPathID)))) {
      throw new IOException(String.format("Snapshot chain corruption. " +
              "Previous snapshotId: %s is set for snapshotId: %s but no " +
              "associated snapshot found in snapshot chain.", prevPathID,
          snapshotID));
    }

    if (prevPathID != null &&
        snapshotChainPath.containsKey(snapshotPath)) {
      snapshotChainPath
          .get(snapshotPath)
          .get(prevPathID)
          .setNextSnapshotID(snapshotID);
    }

    if (!snapshotChainPath.containsKey(snapshotPath)) {
      snapshotChainPath.put(snapshotPath, new LinkedHashMap<>());
    }

    snapshotChainPath
        .get(snapshotPath)
        .put(snapshotID,
            new SnapshotChainInfo(snapshotID, prevPathID, null));

    // set state variable latestPath snapshot entry to this snapshotID
    latestPathSnapshotID.put(snapshotPath, snapshotID);
  }

  private boolean deleteSnapshotGlobal(String snapshotID) throws IOException {
    boolean status = true;
    if (snapshotChainGlobal.containsKey(snapshotID)) {
      // reset prev and next snapshot entries in chain ordered list
      // for node removal
      String next = snapshotChainGlobal.get(snapshotID).getNextSnapshotID();
      String prev = snapshotChainGlobal.get(snapshotID).getPreviousSnapshotID();

      if (prev != null && !snapshotChainGlobal.containsKey(prev)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: %s to be deleted has previous snapshotId: %s " +
                "but associated snapshot is not found in snapshot chain.",
            snapshotID, prev));
      }
      if (next != null && !snapshotChainGlobal.containsKey(next)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: {%s} to be deleted has next snapshotId: %s " +
                "but associated snapshot is not found in snapshot chain.",
            snapshotID, next));
      }
      snapshotChainGlobal.remove(snapshotID);
      if (next != null) {
        snapshotChainGlobal.get(next).setPreviousSnapshotID(prev);
      }
      if (prev != null) {
        snapshotChainGlobal.get(prev).setNextSnapshotID(next);
      }
      // remove from latest list if necessary
      if (latestGlobalSnapshotID.equals(snapshotID)) {
        latestGlobalSnapshotID = prev;
      }
    } else {
      // snapshotID not found in snapshot chain, log warning and return
      LOG.warn("Snapshot chain corruption. SnapshotID: {} is not found in " +
          "snapshot chain.", snapshotID);
      status = false;
    }

    return status;
  }

  private boolean deleteSnapshotPath(String snapshotPath,
                                     String snapshotID) throws IOException {
    if (snapshotChainPath.containsKey(snapshotPath) &&
        snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      // reset prev and next snapshot entries in chain ordered list
      // for node removal
      String nextSnapshotID = snapshotChainPath
          .get(snapshotPath)
          .get(snapshotID)
          .getNextSnapshotID();
      String previousSnapshotID = snapshotChainPath
          .get(snapshotPath)
          .get(snapshotID)
          .getPreviousSnapshotID();

      if (previousSnapshotID != null &&
          !snapshotChainPath.get(snapshotPath)
              .containsKey(previousSnapshotID)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: %s at snapshotPath: %s to be deleted has " +
                "previous snapshotId: %s but associated snapshot is not " +
                "found in snapshot chain.", snapshotID, snapshotPath,
            previousSnapshotID));
      }
      if (nextSnapshotID != null && !snapshotChainPath.get(snapshotPath)
          .containsKey(nextSnapshotID)) {
        throw new IOException(String.format("Snapshot chain corruption. " +
                "SnapshotId: %s at snapshotPath: %s to be deleted has next " +
                "snapshotId: %s but associated snapshot is not found in " +
                "snapshot chain.", snapshotID, snapshotPath,
            nextSnapshotID));
      }

      snapshotChainPath.get(snapshotPath).remove(snapshotID);
      if (nextSnapshotID != null) {
        snapshotChainPath.get(snapshotPath)
            .get(nextSnapshotID)
            .setPreviousSnapshotID(previousSnapshotID);
      }
      if (previousSnapshotID != null) {
        snapshotChainPath.get(snapshotPath)
            .get(previousSnapshotID)
            .setNextSnapshotID(nextSnapshotID);
      }
      // remove path if no entries
      if (snapshotChainPath.get(snapshotPath).isEmpty()) {
        snapshotChainPath.remove(snapshotPath);
      }
      // remove from latest list if necessary
      if (latestPathSnapshotID.get(snapshotPath).equals(snapshotID)) {
        latestPathSnapshotID.remove(snapshotPath);
        if (previousSnapshotID != null) {
          latestPathSnapshotID.put(snapshotPath, previousSnapshotID);
        }
      }
      return true;
    } else {
      // snapshotID not found in snapshot chain, log warning and return
      LOG.warn("Snapshot chain corruption. SnapshotId: {} is not in chain " +
          "found for snapshot path {}.", snapshotID, snapshotPath);
      return false;
    }
  }

  /**
   * Loads the snapshot chain from SnapshotInfo table.
   * @param metadataManager OMMetadataManager
   */
  private void loadFromSnapshotInfoTable(OMMetadataManager metadataManager)
      throws IOException {
    // read from snapshotInfo table to populate
    // snapshot chains - both global and local path
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
        keyIter = metadataManager.getSnapshotInfoTable().iterator()) {
      Map<Long, SnapshotInfo> snaps = new TreeMap<>();
      Table.KeyValue<String, SnapshotInfo> kv;
      snapshotChainGlobal.clear();
      snapshotChainPath.clear();
      latestPathSnapshotID.clear();
      snapshotIdToTableKey.clear();

      while (keyIter.hasNext()) {
        kv = keyIter.next();
        snaps.put(kv.getValue().getCreationTime(), kv.getValue());
      }
      for (SnapshotInfo snapshotInfo : snaps.values()) {
        addSnapshot(snapshotInfo);
      }
    }
  }

  /**
   * Add snapshot to snapshot chain.
   * @param snapshotInfo SnapshotInfo of snapshot to add to chain.
   */
  public void addSnapshot(SnapshotInfo snapshotInfo) throws IOException {
    addSnapshotGlobal(snapshotInfo.getSnapshotID(),
        snapshotInfo.getGlobalPreviousSnapshotID());
    addSnapshotPath(snapshotInfo.getSnapshotPath(),
        snapshotInfo.getSnapshotID(), snapshotInfo.getPathPreviousSnapshotID());
    // store snapshot ID to snapshot DB table key in the map
    snapshotIdToTableKey.put(snapshotInfo.getSnapshotID(),
        snapshotInfo.getTableKey());
  }

  /**
   * Delete snapshot from snapshot chain.
   * @param snapshotInfo SnapshotInfo of snapshot to remove from chain.
   * @return boolean
   */
  public boolean deleteSnapshot(SnapshotInfo snapshotInfo) throws IOException {
    boolean status = deleteSnapshotGlobal(snapshotInfo.getSnapshotID()) &&
        deleteSnapshotPath(snapshotInfo.getSnapshotPath(),
            snapshotInfo.getSnapshotID());
    if (status) {
      snapshotIdToTableKey.remove(snapshotInfo.getSnapshotID());
    }
    return status;
  }

  /**
   * Get latest global snapshot in snapshot chain.
   * @return String, snapshot UUID of latest snapshot in global chain;
   * null if empty chain.
   */
  public String getLatestGlobalSnapshot() {
    return latestGlobalSnapshotID;
  }

  /**
   * Get latest path snapshot in snapshot chain.
   * @param snapshotPath String, snapshot directory path.
   * @return String, snapshot UUID of latest snapshot in path chain;
   * null if empty chain.
   */
  public String getLatestPathSnapshot(String snapshotPath) {
    return latestPathSnapshotID.get(snapshotPath);
  }

  /**
   * Returns true if snapshot from given snapshotID has a
   * next snapshot entry in the global snapshot chain.
   * @param snapshotID String, snapshot UUID
   * @return boolean
   */
  public boolean hasNextGlobalSnapshot(String snapshotID) {
    if (!snapshotChainGlobal.containsKey(snapshotID)) {
      LOG.error("No snapshot for provided snapshotId: {}", snapshotID);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotID));
    }
    return snapshotChainGlobal.get(snapshotID).getNextSnapshotID() != null;
  }

  /**
   * Get next global snapshot in snapshot chain from given snapshot.
   * @param snapshotID String, snapshot UUID
   * @return String, snapshot UUID of next snapshot in chain from
   * snapshotID
   */
  public String nextGlobalSnapshot(String snapshotID) {
    if (!hasNextGlobalSnapshot(snapshotID)) {
      LOG.error("No following snapshot found in snapshot chain for provided " +
              "snapshotId: {}.", snapshotID);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotID));
    }
    return snapshotChainGlobal.get(snapshotID).getNextSnapshotID();
  }

  /**
   * Returns true if snapshot from given snapshotID has a
   * previous snapshot entry in the global snapshot chain.
   * @param snapshotID String, snapshot UUID
   * @return boolean
   */
  public boolean hasPreviousGlobalSnapshot(String snapshotID) {
    if (!snapshotChainGlobal.containsKey(snapshotID)) {
      LOG.error("No snapshot found in snapshot chain for provided " +
          "snapshotId: {}.", snapshotID);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotID));
    }

    return snapshotChainGlobal.get(snapshotID).getPreviousSnapshotID() != null;
  }

  /**
   * Get previous global snapshot in snapshot chain from given snapshot.
   * @param snapshotID String, snapshot UUID
   * @return String, snapshot UUID of previous snapshot in chain from
   * snapshotID
   */
  public String previousGlobalSnapshot(String snapshotID) {
    if (!hasPreviousGlobalSnapshot(snapshotID)) {
      LOG.error("No preceding snapshot found in snapshot chain for provided " +
          "snapshotId: {}.", snapshotID);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
          "found in snapshot chain.", snapshotID));
    }
    return snapshotChainGlobal.get(snapshotID).getPreviousSnapshotID();
  }

  /**
   * Returns true if snapshot path from given snapshotID has a
   * next snapshot entry in the path snapshot chain.
   * @param snapshotPath String, snapshot directory path.
   * @param snapshotID String, snapshot UUID
   * @return boolean
   */
  public boolean hasNextPathSnapshot(String snapshotPath, String snapshotID) {
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("No snapshot found for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotID, snapshotPath);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
              "found in snapshot chain for snapshotPath: %s.", snapshotID,
          snapshotPath));
    }

    return snapshotChainPath
        .get(snapshotPath)
        .get(snapshotID)
        .getNextSnapshotID() != null;
  }

  /**
   * Returns true if snapshot path from given snapshotID has a
   * previous snapshot entry in the path snapshot chain.
   * @param snapshotPath String, snapshot directory path.
   * @param snapshotID String, snapshot UUID
   * @return boolean
   */
  public boolean hasPreviousPathSnapshot(String snapshotPath,
                                         String snapshotID) {
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("No snapshot found for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotID, snapshotPath);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
              "found in snapshot chain for snapshotPath: %s.", snapshotID,
          snapshotPath));
    }
    return snapshotChainPath.get(snapshotPath)
        .get(snapshotID)
        .getPreviousSnapshotID() != null;
  }

  /**
   * Get next path snapshot in snapshot chain from given snapshot.
   * @param snapshotPath String, snapshot directory path.
   * @param snapshotID String, snapshot UUID
   * @return String, snapshot UUID of next snapshot in chain from
   * snapshotID
   */
  public String nextPathSnapshot(String snapshotPath, String snapshotID) {
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("No snapshot found for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotID, snapshotPath);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
              "found in snapshot chain for snapshotPath: %s.", snapshotID,
          snapshotPath));
    }
    if (snapshotChainPath.get(snapshotPath).get(snapshotID)
        .getNextSnapshotID() == null) {
      LOG.error("No following snapshot for provided snapshotId {} and " +
          "snapshotPath {}.", snapshotID, snapshotPath);
      throw new NoSuchElementException(String.format("No following snapshot " +
          "found in snapshot chain for snapshotId: %s and snapshotPath: " +
          "%s.", snapshotID, snapshotPath));
    }
    return snapshotChainPath.get(snapshotPath)
        .get(snapshotID)
        .getNextSnapshotID();
  }

  /**
   * Get previous path snapshot in snapshot chain from given snapshot.
   * @param snapshotPath String, snapshot directory path.
   * @param snapshotID String, snapshot UUID
   * @return String, snapshot UUID of previous snapshot in chain from
   * snapshotID
   */
  public String previousPathSnapshot(String snapshotPath, String snapshotID) {
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("No snapshot found for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotID, snapshotPath);
      throw new NoSuchElementException(String.format("SnapshotId: %s is not " +
              "found in snapshot chain for snapshotPath: %s.", snapshotID,
          snapshotPath));
    }
    if (snapshotChainPath
        .get(snapshotPath)
        .get(snapshotID)
        .getPreviousSnapshotID() == null) {
      LOG.error("No preceding snapshot for provided snapshotId: {} and " +
          "snapshotPath: {}", snapshotID, snapshotPath);
      throw new NoSuchElementException(String.format("No preceding snapshot " +
              "found in snapshot chain for snapshotId: %s and snapshotPath: " +
              "%s.", snapshotID, snapshotPath));
    }
    return snapshotChainPath
        .get(snapshotPath)
        .get(snapshotID)
        .getPreviousSnapshotID();
  }

  public String getTableKey(String snapshotId) {
    return snapshotIdToTableKey.get(snapshotId);
  }

  @VisibleForTesting
  public void loadSnapshotInfo(OMMetadataManager metadataManager)
      throws IOException {
    loadFromSnapshotInfoTable(metadataManager);
  }

  @VisibleForTesting
  public LinkedHashMap<String, SnapshotChainInfo> getSnapshotChainPath(
      String path) {
    return snapshotChainPath.get(path);
  }
}
