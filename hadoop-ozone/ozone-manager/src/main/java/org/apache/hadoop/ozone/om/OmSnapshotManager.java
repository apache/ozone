/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffReport;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;


/**
 * This class is used to manage/create OM snapshots.
 */
public final class OmSnapshotManager {
  public static final String OM_HARDLINK_FILE = "hardLinkFile";
  private final OzoneManager ozoneManager;
  private final SnapshotDiffManager snapshotDiffManager;
  private final LoadingCache<String, OmSnapshot> snapshotCache;

  private static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotManager.class);

  OmSnapshotManager(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;

    this.snapshotDiffManager = new SnapshotDiffManager();

    // size of lru cache
    int cacheSize = ozoneManager.getConfiguration().getInt(
        OzoneConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE,
        OzoneConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT);

    CacheLoader<String, OmSnapshot> loader;
    loader = new CacheLoader<String, OmSnapshot>() {
      @Override

      // load the snapshot into the cache if not already there
      @Nonnull
      public OmSnapshot load(@Nonnull String snapshotTableKey)
          throws IOException {
        SnapshotInfo snapshotInfo;
        // see if the snapshot exists
        snapshotInfo = getSnapshotInfo(snapshotTableKey);

        // read in the snapshot
        OzoneConfiguration conf = ozoneManager.getConfiguration();
        OMMetadataManager snapshotMetadataManager;

        // Create the snapshot metadata manager by finding the corresponding
        // RocksDB instance, creating an OmMetadataManagerImpl instance based on
        // that
        try {
          snapshotMetadataManager = OmMetadataManagerImpl
              .createSnapshotMetadataManager(
              conf, snapshotInfo.getCheckpointDirName());
        } catch (IOException e) {
          LOG.error("Failed to retrieve snapshot: {}, {}", snapshotTableKey, e);
          throw e;
        }

        // create the other manager instances based on snapshot metadataManager
        PrefixManagerImpl pm = new PrefixManagerImpl(snapshotMetadataManager,
            false);
        KeyManagerImpl km = new KeyManagerImpl(null,
            ozoneManager.getScmClient(), snapshotMetadataManager, conf,
            ozoneManager.getBlockTokenSecretManager(),
            ozoneManager.getKmsProvider(), ozoneManager.getPerfMetrics());

        return new OmSnapshot(km, pm, ozoneManager,
            snapshotInfo.getVolumeName(),
            snapshotInfo.getBucketName(),
            snapshotInfo.getName());
      }
    };

    RemovalListener<String, OmSnapshot> removalListener
        = notification -> {
          try {
            // close snapshot's rocksdb on eviction
            notification.getValue().close();
          } catch (IOException e) {
            LOG.error("Failed to close snapshot: {} {}",
                notification.getKey(), e);
          }
        };
    // init LRU cache
    snapshotCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .removalListener(removalListener)
        .build(loader);
  }

  /**
   * Creates snapshot checkpoint that corresponds to snapshotInfo.
   * @param omMetadataManager the metadata manager
   * @param snapshotInfo The metadata of snapshot to be created
   * @return instance of DBCheckpoint
   */
  public static DBCheckpoint createOmSnapshotCheckpoint(
      OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();

    final long dbLatestSequenceNumber = snapshotInfo.getDbTxSequenceNumber();

    final RocksDBCheckpointDiffer dbCpDiffer =
        omMetadataManager.getStore().getRocksDBCheckpointDiffer();

    final DBCheckpoint dbCheckpoint = store.getSnapshot(
        snapshotInfo.getCheckpointDirName());

    // Write snapshot generation (latest sequence number) to compaction log.
    // This will be used for DAG reconstruction as snapshotGeneration.
    dbCpDiffer.appendSequenceNumberToCompactionLog(dbLatestSequenceNumber);

    // Set compaction log filename to the latest DB sequence number
    // right after taking the RocksDB checkpoint for Ozone snapshot.
    //
    // Note it doesn't matter if sequence number hasn't increased (even though
    // it shouldn't happen), since the writer always appends the file.
    dbCpDiffer.setCurrentCompactionLog(dbLatestSequenceNumber);

    return dbCheckpoint;
  }

  @VisibleForTesting
  static Object getINode(Path file) throws IOException {
    return Files.readAttributes(
        file, BasicFileAttributes.class).fileKey();
  }

  // Get OmSnapshot if the keyname has ".snapshot" key indicator
  public IOmMetadataReader checkForSnapshot(String volumeName,
                                     String bucketName, String keyname)
      throws IOException {
    if (keyname == null) {
      return ozoneManager.getOmMetadataReader();
    }

    // see if key is for a snapshot
    String[] keyParts = keyname.split("/");
    if (isSnapshotKey(keyParts)) {
      String snapshotName = keyParts[1];
      if (snapshotName == null || snapshotName.isEmpty()) {
        // don't allow snapshot indicator without snapshot name
        throw new OMException(INVALID_KEY_NAME);
      }
      String snapshotTableKey = SnapshotInfo.getTableKey(volumeName,
          bucketName, snapshotName);

      // retrieve the snapshot from the cache
      try {
        return snapshotCache.get(snapshotTableKey);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    } else {
      return ozoneManager.getOmMetadataReader();
    }
  }

  public SnapshotInfo getSnapshotInfo(String volumeName,
                                      String bucketName, String snapshotName)
      throws IOException {
    return getSnapshotInfo(SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName));
  }

  private SnapshotInfo getSnapshotInfo(String key) throws IOException {
    SnapshotInfo snapshotInfo;
    try {
      snapshotInfo = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(key);
    } catch (IOException e) {
      LOG.error("Snapshot {}: not found: {}", key, e);
      throw e;
    }
    if (snapshotInfo == null) {
      throw new OMException(KEY_NOT_FOUND);
    }
    return snapshotInfo;
  }

  public static String getSnapshotPrefix(String snapshotName) {
    return OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX +
        snapshotName + OM_KEY_PREFIX;
  }

  public static String getSnapshotPath(OzoneConfiguration conf,
                                     SnapshotInfo snapshotInfo) {
    return OMStorage.getOmDbDir(conf) +
        OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX +
        OM_DB_NAME + snapshotInfo.getCheckpointDirName();
  }

  public static boolean isSnapshotKey(String[] keyParts) {
    return (keyParts.length > 1) &&
        (keyParts[0].compareTo(OM_SNAPSHOT_INDICATOR) == 0);
  }

  public SnapshotDiffReport getSnapshotDiffReport(final String volume,
                                                  final String bucket,
                                                  final String fromSnapshot,
                                                  final String toSnapshot)
      throws IOException {
    // Validate fromSnapshot and toSnapshot
    final SnapshotInfo fsInfo = getSnapshotInfo(volume, bucket, fromSnapshot);
    final SnapshotInfo tsInfo = getSnapshotInfo(volume, bucket, toSnapshot);
    verifySnapshotInfoForSnapDiff(fsInfo, tsInfo);

    final String fsKey = SnapshotInfo.getTableKey(volume, bucket, fromSnapshot);
    final String tsKey = SnapshotInfo.getTableKey(volume, bucket, toSnapshot);
    try {
      final OmSnapshot fs = snapshotCache.get(fsKey);
      final OmSnapshot ts = snapshotCache.get(tsKey);
      return snapshotDiffManager.getSnapshotDiffReport(volume, bucket, fs, ts);
    } catch (ExecutionException | RocksDBException e) {
      throw new IOException(e.getCause());
    }
  }

  private void verifySnapshotInfoForSnapDiff(final SnapshotInfo fromSnapshot,
                                             final SnapshotInfo toSnapshot)
      throws IOException {
    if ((fromSnapshot.getSnapshotStatus() != SnapshotStatus.SNAPSHOT_ACTIVE) ||
        (toSnapshot.getSnapshotStatus() != SnapshotStatus.SNAPSHOT_ACTIVE)) {
      // TODO: throw custom snapshot exception.
      throw new IOException("Cannot generate snapshot diff for non-active " +
          "snapshots.");
    }
    if (fromSnapshot.getCreationTime() > toSnapshot.getCreationTime()) {
      throw new IOException("fromSnapshot:" + fromSnapshot.getName() +
          " should be older than to toSnapshot:" + toSnapshot.getName());
    }
  }
  /**
   * Create file of links to add to tarball.
   * Format of entries are either:
   * dir1/fileTo fileFrom
   *    for files in active db or:
   * dir1/fileTo dir2/fileFrom
   *    for files in another directory, (either another snapshot dir or
   *    sst compaction backup directory)
   * @param truncateLength - Length of initial path to trim in file path.
   * @param hardLinkFiles - Map of link->file paths.
   * @return Path to the file of links created.
   */
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  static Path createHardLinkList(int truncateLength,
                                  Map<Path, Path> hardLinkFiles)
      throws IOException {
    Path data = Files.createTempFile("data", "txt");
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Path, Path> entry : hardLinkFiles.entrySet()) {
      String fixedFile = truncateFileName(truncateLength, entry.getValue());
      // If this file is from the active db, strip the path.
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        fixedFile = Paths.get(fixedFile).getFileName().toString();
      }
      sb.append(truncateFileName(truncateLength, entry.getKey()))
          .append("\t")
          .append(fixedFile)
          .append("\n");
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8));
    return data;
  }

  /**
   * Get the filename without the introductory metadata directory.
   *
   * @param truncateLength Length to remove.
   * @param file File to remove prefix from.
   * @return Truncated string.
   */
  static String truncateFileName(int truncateLength, Path file) {
    return file.toString().substring(truncateLength);
  }

  /**
   * Create hard links listed in OM_HARDLINK_FILE.
   *
   * @param dbPath Path to db to have links created.
   */
  static void createHardLinks(Path dbPath) throws IOException {
    File hardLinkFile = new File(dbPath.toString(),
        OM_HARDLINK_FILE);
    if (hardLinkFile.exists()) {
      // Read file.
      List<String> lines =
          Files.lines(hardLinkFile.toPath()).collect(Collectors.toList());

      // Create a link for each line.
      for (String l : lines) {
        String from = l.split("\t")[1];
        String to = l.split("\t")[0];
        Path fullFromPath = getFullPath(dbPath, from);
        Path fullToPath = getFullPath(dbPath, to);
        Files.createLink(fullToPath, fullFromPath);
      }
      if (!hardLinkFile.delete()) {
        throw new IOException(
            "Failed to delete: " + hardLinkFile);
      }
    }
  }


  // Prepend the full path to the hard link entry entry.
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  private static Path getFullPath(Path dbPath, String fileName) {
    File file = new File(fileName);
    // If there is no directory then this file belongs in the db.
    if (file.getName().equals(fileName)) {
      return Paths.get(dbPath.toString(), fileName);
    }
    // Else this file belong in a directory parallel to the db.
    return Paths.get(dbPath.getParent().toString(), fileName);
  }

}
