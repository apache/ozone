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

package org.apache.hadoop.hdds.utils;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.ratisSnapshotComplete;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_CANDIDATE_DIR;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RocksDB specified snapshot provider.
 * Supports Incremental Snapshot and Full Snapshot.
 *
 * The process is as the followings:
 * 1. Download the snapshot file from the leader
 * 2. Untar the snapshot file to candidate dir
 * 3. Return the candidate dir as DBCheckpoint
 *
 * The difference between incremental and full snapshot is whether to send
 * the existing SST file list to the leader or not.
 *
 */
public abstract class RDBSnapshotProvider implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBSnapshotProvider.class);

  private final File snapshotDir;
  private final File candidateDir;
  private final String dbName;
  private final AtomicReference<String> lastLeaderRef;
  private final AtomicLong numDownloaded;
  private FaultInjector injector;
  // The number of times init() is called
  private final AtomicLong initCount;

  public RDBSnapshotProvider(File snapshotDir, String dbName) {
    this.snapshotDir = snapshotDir;
    this.candidateDir = new File(snapshotDir, dbName + SNAPSHOT_CANDIDATE_DIR);
    this.dbName = dbName;
    this.injector = null;
    this.lastLeaderRef = new AtomicReference<>(null);
    this.numDownloaded = new AtomicLong();
    this.initCount = new AtomicLong();
    init();
  }

  /**
   * Initialize or reinitialize the RDB snapshot provider.
   */
  public synchronized void init() {
    // check parent snapshot dir
    if (!snapshotDir.exists()) {
      HddsUtils.createDir(snapshotDir.toString());
    }

    LOG.info("Cleaning up the candidate dir: {}", candidateDir);
    // cleanup candidate dir
    if (candidateDir.exists()) {
      FileUtil.fullyDeleteContents(candidateDir);
    } else {
      // create candidate dir
      HddsUtils.createDir(candidateDir.toString());
    }

    // reset leader info
    lastLeaderRef.set(null);
    initCount.incrementAndGet();
  }

  /**
   * Download the latest DB snapshot(checkpoint) from the Leader.
   *
   * @param leaderNodeID the ID of leader node
   * @return {@link DBCheckpoint}
   * @throws IOException
   */
  public DBCheckpoint downloadDBSnapshotFromLeader(String leaderNodeID)
      throws IOException {
    LOG.info("Prepare to download the snapshot from leader OM {} and " +
        "reloading state from the snapshot.", leaderNodeID);
    checkLeaderConsistency(leaderNodeID);
    int numParts = 0;

    while (true) {
      String snapshotFileName = getSnapshotFileName(leaderNodeID);
      File targetFile = new File(snapshotDir, snapshotFileName);
      downloadSnapshot(leaderNodeID, targetFile);
      LOG.info("Successfully download the latest snapshot {} from leader OM: {}, part : {}",
          targetFile, leaderNodeID, numParts);
      numParts++;

      numDownloaded.incrementAndGet();
      injectPause();

      Path unTarredDb = untarContentsOfTarball(targetFile,
          candidateDir, true);
      LOG.info("Successfully untar the downloaded snapshot {} at {}.",
          targetFile, unTarredDb.toAbsolutePath());
      if (ratisSnapshotComplete(unTarredDb)) {
        LOG.info("Ratis snapshot transfer is complete.");
        return getCheckpointFromUntarredDb(unTarredDb);
      }
    }
  }

  /**
   * Clean up the candidate DB for the following reason:
   * 1. If leader changes when installing incremental snapshot
   *    Notice: here prevents downloading the error IC from the new leader,
   *    instead, will ask for a full snapshot directly
   * 2. Ready to download the full snapshot
   *
   * @param currentLeader the ID of leader node
   */
  @VisibleForTesting
  void checkLeaderConsistency(String currentLeader) throws IOException {
    String lastLeader = lastLeaderRef.get();
    if (lastLeader != null) {
      if (!lastLeader.equals(currentLeader)) {
        LOG.info("Last leader for install snapshot is {}, but current leader " +
            "is {}. ", lastLeader, currentLeader);
        init();
        lastLeaderRef.set(currentLeader);
      }
      return;
    }

    List<String> files = HAUtils.getExistingFiles(candidateDir);
    if (!files.isEmpty()) {
      LOG.warn("Candidate DB directory {} is not empty when last leader is " +
          "null.", candidateDir);
      init();
    }
    lastLeaderRef.set(currentLeader);
  }

  /**
   * Get the snapshot file name.
   *
   * @param leaderNodeID the ID of leader node
   * @return snapshot file name
   */
  public String getSnapshotFileName(String leaderNodeID) {
    String snapshotTime = Long.toString(System.currentTimeMillis());
    return dbName + "-" + leaderNodeID + "-" + snapshotTime + ".tar";
  }

  /**
   * convert untarredDbDir to to {@link RocksDBCheckpoint}.
   *
   * @return {@link RocksDBCheckpoint}
   * @throws IOException
   */
  public DBCheckpoint getCheckpointFromUntarredDb(Path untarredDbDir) throws IOException {
    return new RocksDBCheckpoint(untarredDbDir);
  }

  /**
   *
   * Untar the downloaded snapshot.
   * @param snapshot the downloaded snapshot tar file
   * @param untarDir the directory to place the untarred files
   * @param deleteSnapshot whether to delete the downloaded snapshot tar file
   * @return  path of untarred dbDir.
   * @throws IOException
   */
  private Path untarContentsOfTarball(File snapshot,
      File untarDir, boolean deleteSnapshot) throws IOException {
    // Untar the checkpoint file.
    Path untarredDbDir = untarDir.toPath();
    FileUtil.unTar(snapshot, untarredDbDir.toFile());

    if (deleteSnapshot) {
      FileUtil.fullyDelete(snapshot);
    }
    return untarredDbDir;
  }

  /**
   * The abstract method to download the snapshot.
   * Could be implemented in HTTP, GRPC, etc.
   *
   * @param leaderNodeID the ID of leader node
   * @param targetFile   the snapshot file to be downloaded in
   * @throws IOException
   */
  public abstract void downloadSnapshot(String leaderNodeID, File targetFile)
      throws IOException;

  /**
   * Inject pause for test only.
   *
   * @throws IOException
   */
  private void injectPause() throws IOException {
    if (injector != null) {
      injector.pause();
    }
  }

  @VisibleForTesting
  public File getSnapshotDir() {
    return snapshotDir;
  }

  @VisibleForTesting
  public File getCandidateDir() {
    return candidateDir;
  }

  @VisibleForTesting
  public FaultInjector getInjector() {
    return injector;
  }

  @VisibleForTesting
  public void setInjector(FaultInjector injector) {
    this.injector = injector;
  }

  @VisibleForTesting
  public long getNumDownloaded() {
    return numDownloaded.get();
  }

  @VisibleForTesting
  public long getInitCount() {
    return initCount.get();
  }
}
