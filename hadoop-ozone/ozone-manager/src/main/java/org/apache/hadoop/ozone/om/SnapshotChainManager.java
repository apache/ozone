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
 *
 * The snapshot chain maintains the in-memory sequence of snapshots
 * created in chronological order.  There are two such snapshots maintained
 * i.) Path based snapshot chain, sequence of snapshots created for a
 * given /volume/bucket
 * ii.) Global snapshot chain, sequence of all snapshots created in order
 *
 * On start, the snapshot chains are initialized from the on disk
 * SnapshotInfoTable from the om RocksDB.
 */
public class SnapshotChainManager {
  private LinkedHashMap<String, SnapshotChainInfo>  snapshotChainGlobal;
  private Map<String, LinkedHashMap<String, SnapshotChainInfo>>
      snapshotChainPath;
  private Map<String, String> latestPathSnapshotID;
  private String latestGlobalSnapshotID;
  private Map<String, String> snapshotIdToTableKey;
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotChainManager.class);

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
      throw new IOException("Snapshot Chain corruption: "
          + " previous snapshotID given but no associated snapshot "
          + "found in snapshot chain: SnapshotID "
          + prevGlobalID);
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
      throw new IOException("Snapshot Chain corruption: "
          + "previous snapshotID given but no associated snapshot "
          + "found in snapshot chain: SnapshotID "
          + prevPathID);
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
        throw new IOException("Snapshot chain corruption: snapshot node to be "
            + "deleted has prev node element not found in snapshot chain: "
            + "SnapshotID " + prev);
      }
      if (next != null && !snapshotChainGlobal.containsKey(next)) {
        throw new IOException("Snapshot chain corruption: snapshot node to be "
            + "deleted has next node element not found in snapshot chain: "
            + "SnapshotID " + next);
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
      LOG.warn("Snapshot chain: snapshotID not found: SnapshotID {}",
          snapshotID);
      status = false;
    }

    return status;
  }

  private boolean deleteSnapshotPath(String snapshotPath,
                                     String snapshotID) throws IOException {
    boolean status = true;
    if (snapshotChainPath.containsKey(snapshotPath) &&
        snapshotChainPath
            .get(snapshotPath)
            .containsKey(snapshotID)) {
      // reset prev and next snapshot entries in chain ordered list
      // for node removal
      String next = snapshotChainPath
          .get(snapshotPath)
          .get(snapshotID)
          .getNextSnapshotID();
      String prev = snapshotChainPath
          .get(snapshotPath)
          .get(snapshotID)
          .getPreviousSnapshotID();

      if (prev != null &&
          !snapshotChainPath
              .get(snapshotPath)
              .containsKey(prev)) {
        throw new IOException("Snapshot chain corruption: snapshot node to "
            + "be deleted has prev node element not found in snapshot "
            + "chain: Snapshot path " + snapshotPath + ", SnapshotID "
            + prev);
      }
      if (next != null && !snapshotChainPath
          .get(snapshotPath)
          .containsKey(next)) {
        throw new IOException("Snapshot chain corruption: snapshot node to "
            + "be deleted has next node element not found in snapshot "
            + "chain:  Snapshot path " + snapshotPath + ", SnapshotID "
            + next);
      }
      snapshotChainPath
          .get(snapshotPath)
          .remove(snapshotID);
      if (next != null) {
        snapshotChainPath
            .get(snapshotPath)
            .get(next)
            .setPreviousSnapshotID(prev);
      }
      if (prev != null) {
        snapshotChainPath
            .get(snapshotPath)
            .get(prev)
            .setNextSnapshotID(next);
      }
      // remove path if no entries
      if (snapshotChainPath.get(snapshotPath).isEmpty()) {
        snapshotChainPath.remove(snapshotPath);
      }
      // remove from latest list if necessary
      if (latestPathSnapshotID.get(snapshotPath).equals(snapshotID)) {
        latestPathSnapshotID.remove(snapshotPath);
        if (prev != null) {
          latestPathSnapshotID.put(snapshotPath, prev);
        }
      }

    } else {
      // snapshotID not found in snapshot chain, log warning and return
      LOG.warn("Snapshot chain: snapshotID not found: Snapshot path {}," +
              " SnapshotID {}",
          snapshotPath, snapshotID);
      status = false;
    }

    return status;
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
      for (SnapshotInfo sinfo : snaps.values()) {
        addSnapshot(sinfo);
      }
    }
  }

  /**
   * Add snapshot to snapshot chain.
   * @param sinfo SnapshotInfo of snapshot to add to chain.
   */
  public void addSnapshot(SnapshotInfo sinfo) throws IOException {
    addSnapshotGlobal(sinfo.getSnapshotID(),
        sinfo.getGlobalPreviousSnapshotID());
    addSnapshotPath(sinfo.getSnapshotPath(),
        sinfo.getSnapshotID(),
        sinfo.getPathPreviousSnapshotID());
    // store snapshot ID to snapshot DB table key in the map
    snapshotIdToTableKey.put(sinfo.getSnapshotID(), sinfo.getTableKey());
  }

  /**
   * Delete snapshot from snapshot chain.
   * @param sinfo SnapshotInfo of snapshot to remove from chain.
   * @return boolean
   */
  public boolean deleteSnapshot(SnapshotInfo sinfo) throws IOException {
    boolean status;

    status = deleteSnapshotGlobal(sinfo.getSnapshotID()) &&
        deleteSnapshotPath(sinfo.getSnapshotPath(), sinfo.getSnapshotID());
    if (status) {
      snapshotIdToTableKey.remove(sinfo.getSnapshotID());
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
  public boolean hasNextGlobalSnapshot(String snapshotID)
          throws NoSuchElementException {
    boolean hasNext = false;
    if (!snapshotChainGlobal.containsKey(snapshotID)) {
      LOG.error("no snapshot for provided snapshotID {}", snapshotID);
      throw new NoSuchElementException("No snapshot: " + snapshotID);
    }
    if (snapshotChainGlobal
        .get(snapshotID)
        .getNextSnapshotID() != null) {
      hasNext = true;
    }
    return hasNext;
  }

  /**
   * Get next global snapshot in snapshot chain from given snapshot.
   * @param snapshotID String, snapshot UUID
   * @return String, snapshot UUID of next snapshot in chain from
   * snapshotID
   */
  public String nextGlobalSnapshot(String snapshotID)
          throws NoSuchElementException {
    if (!hasNextGlobalSnapshot(snapshotID)) {
      LOG.error("no following snapshot for provided snapshotID {}", snapshotID);
      throw new NoSuchElementException("no following snapshot from: "
          + snapshotID);
    }
    return snapshotChainGlobal
        .get(snapshotID)
        .getNextSnapshotID();
  }

  /**
   * Returns true if snapshot from given snapshotID has a
   * previous snapshot entry in the global snapshot chain.
   * @param snapshotID String, snapshot UUID
   * @return boolean
   */
  public boolean hasPreviousGlobalSnapshot(String snapshotID)
          throws NoSuchElementException {
    boolean hasPrevious = false;
    if (!snapshotChainGlobal.containsKey(snapshotID)) {
      LOG.error("no snapshot for provided snapshotID {}", snapshotID);
      throw new NoSuchElementException("No snapshot: " + snapshotID);
    }
    if (snapshotChainGlobal
        .get(snapshotID)
        .getPreviousSnapshotID() != null) {
      hasPrevious = true;
    }
    return hasPrevious;
  }

  /**
   * Get previous global snapshot in snapshot chain from given snapshot.
   * @param snapshotID String, snapshot UUID
   * @return String, snapshot UUID of previous snapshot in chain from
   * snapshotID
   */
  public String previousGlobalSnapshot(String snapshotID)
      throws NoSuchElementException {
    if (!hasPreviousGlobalSnapshot(snapshotID)) {
      LOG.error("no preceeding snapshot for provided snapshotID {}",
          snapshotID);
      throw new NoSuchElementException("No preceeding snapshot from: "
          + snapshotID);
    }
    return snapshotChainGlobal
        .get(snapshotID)
        .getPreviousSnapshotID();
  }

  /**
   * Returns true if snapshot path from given snapshotID has a
   * next snapshot entry in the path snapshot chain.
   * @param snapshotPath String, snapshot directory path.
   * @param snapshotID String, snapshot UUID
   * @return boolean
   */
  public boolean hasNextPathSnapshot(String snapshotPath, String snapshotID)
          throws NoSuchElementException {
    boolean hasNext = false;
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("no snapshot for provided snapshotPath {} and "
          + " snapshotID {}", snapshotPath, snapshotID);
      throw new NoSuchElementException("No such snapshot: "
          + snapshotID + "for path: " + snapshotPath);
    }
    if (snapshotChainPath
        .get(snapshotPath)
        .get(snapshotID)
        .getNextSnapshotID() != null) {
      hasNext = true;
    }
    return hasNext;
  }

  /**
   * Returns true if snapshot path from given snapshotID has a
   * previous snapshot entry in the path snapshot chain.
   * @param snapshotPath String, snapshot directory path.
   * @param snapshotID String, snapshot UUID
   * @return boolean
   */
  public boolean hasPreviousPathSnapshot(String snapshotPath, String snapshotID)
      throws NoSuchElementException {
    boolean hasPrevious = false;
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("no snapshot for provided snapshotPath {} and "
          + " snapshotID {}", snapshotPath, snapshotID);
      throw new NoSuchElementException("No snapshot: " + snapshotID
          + " for snapshot path: " + snapshotPath);
    }
    if (snapshotChainPath
        .get(snapshotPath)
        .get(snapshotID)
        .getPreviousSnapshotID() != null) {
      hasPrevious = true;
    }
    return hasPrevious;
  }

  /**
   * Get next path snapshot in snapshot chain from given snapshot.
   * @param snapshotPath String, snapshot directory path.
   * @param snapshotID String, snapshot UUID
   * @return String, snapshot UUID of next snapshot in chain from
   * snapshotID
   */
  public String nextPathSnapshot(String snapshotPath, String snapshotID)
      throws NoSuchElementException {
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("no snapshot for provided snapshotPath {} and "
          + " snapshotID {}", snapshotPath, snapshotID);
      throw new NoSuchElementException("No snapshot: " + snapshotID
          + " for snapshot path:" + snapshotPath);
    }
    if (snapshotChainPath
        .get(snapshotPath)
        .get(snapshotID)
        .getNextSnapshotID() == null) {
      LOG.error("no following snapshot for provided snapshotPath {}, "
          + "snapshotID {}", snapshotPath, snapshotID);
      throw new NoSuchElementException("no following snapshot from: "
          + snapshotID + "for snapshot path:" + snapshotPath);
    }
    return snapshotChainPath
        .get(snapshotPath)
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
  public String previousPathSnapshot(String snapshotPath, String snapshotID)
      throws NoSuchElementException {
    if (!snapshotChainPath.containsKey(snapshotPath) ||
        !snapshotChainPath.get(snapshotPath).containsKey(snapshotID)) {
      LOG.error("no snapshot for provided snapshotPath {} and "
          + " snapshotID {}", snapshotPath, snapshotID);
      throw new NoSuchElementException("No snapshot: " + snapshotID
          + " for snapshot path:" + snapshotPath);
    }
    if (snapshotChainPath
        .get(snapshotPath)
        .get(snapshotID)
        .getPreviousSnapshotID() == null) {
      LOG.error("no preceeding snapshot for provided snapshotPath {}, "
          + "snapshotID {}", snapshotPath, snapshotID);
      throw new NoSuchElementException("no preceeding snapshot from: "
          + snapshotID + "for snapshot path:" + snapshotPath);
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

}
