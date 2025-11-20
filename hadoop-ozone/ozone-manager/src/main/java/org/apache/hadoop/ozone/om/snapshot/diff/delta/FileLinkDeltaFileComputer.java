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

package org.apache.hadoop.ozone.om.snapshot.diff.delta;

import static java.nio.file.Files.createDirectories;
import static org.apache.commons.io.FilenameUtils.getExtension;
import static org.apache.commons.io.file.PathUtils.deleteDirectory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code FileLinkDeltaFileComputer} is an abstract class that provides a
 * base implementation for the {@code DeltaFileComputer} interface. It is
 * responsible for computing delta files (a list of files if read completely would be able to completely
 * compute all the key changes between two snapshots). Hard links to the
 * relevant source files in a specified delta directory, enabling a compact
 * representation of changes between snapshots.
 *
 * This class encapsulates the logic for managing snapshots and metadata,
 * creating hard links for delta representation, and reporting activity
 * during the computation process.
 */
public abstract class FileLinkDeltaFileComputer implements DeltaFileComputer {

  private static final Logger LOG = LoggerFactory.getLogger(FileLinkDeltaFileComputer.class);
  private final OmSnapshotManager omSnapshotManager;
  private final OMMetadataManager activeMetadataManager;
  private final Consumer<SubStatus> activityReporter;
  private final Path tmpDeltaFileLinkDir;
  private final AtomicInteger linkFileCounter = new AtomicInteger(0);

  FileLinkDeltaFileComputer(OmSnapshotManager snapshotManager, OMMetadataManager activeMetadataManager,
      Path deltaDirPath, Consumer<SubStatus> activityReporter) throws IOException {
    this.tmpDeltaFileLinkDir = deltaDirPath.toAbsolutePath();
    this.omSnapshotManager = snapshotManager;
    this.activityReporter = activityReporter;
    this.activeMetadataManager = activeMetadataManager;
    createDirectories(tmpDeltaFileLinkDir);
  }

  /**
   * Computes the delta files between two snapshots based on the provided parameters.
   * The method determines the differences in data between the `fromSnapshot` and
   * `toSnapshot` and generates a mapping of paths to pairs consisting of a resolved
   * path and corresponding SST file information.
   *
   * @param fromSnapshot the source snapshot from which changes are calculated
   * @param toSnapshot the target snapshot up to which changes are calculated
   * @param tablesToLookup a set of table names to filter the tables that should be considered
   * @param tablePrefixInfo information about table prefixes to apply during computation
   * @return an Optional containing a map where the key is the delta file path, and the value
   *         is a pair consisting of a resolved path and the corresponding SST file information.
   *         Return empty if the delta files could not be computed.
   * @throws IOException if an I/O error occurs during the computation process
   */
  abstract Optional<Map<Path, Pair<Path, SstFileInfo>>> computeDeltaFiles(SnapshotInfo fromSnapshot,
      SnapshotInfo toSnapshot, Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) throws IOException;

  @Override
  public final Collection<Pair<Path, SstFileInfo>> getDeltaFiles(SnapshotInfo fromSnapshot,
      SnapshotInfo toSnapshot, Set<String> tablesToLookup) throws IOException {
    TablePrefixInfo tablePrefixInfo = activeMetadataManager.getTableBucketPrefix(fromSnapshot.getVolumeName(),
        fromSnapshot.getBucketName());
    return computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup,
        tablePrefixInfo).map(Map::values).orElseThrow(() -> new IOException(String.format(
            "Failed to compute delta files for snapshots %s and %s tablesToLookup: %s", fromSnapshot, toSnapshot,
        tablesToLookup)));
  }

  void updateActivity(SubStatus status) {
    activityReporter.accept(status);
  }

  Path createLink(Path path) throws IOException {
    Path source = path.toAbsolutePath();
    Path link;
    boolean createdLink = false;
    Path fileName = source.getFileName();
    if (source.getFileName() == null) {
      throw new IOException("Unable to create link for path " + source + " since it has no file name");
    }
    String extension = getExtension(fileName.toString());
    extension = StringUtils.isBlank(extension) ? "" : ("." + extension);
    do {
      link = tmpDeltaFileLinkDir.resolve(linkFileCounter.incrementAndGet() + extension);
      try {
        Files.createLink(link, source);
        createdLink = true;
      } catch (FileAlreadyExistsException ignored) {
        LOG.info("File for source {} already exists: at {}. Will attempt to create link with a different path", source,
            link);
      }
    } while (!createdLink);
    return link;
  }

  ReadableOmSnapshotLocalDataProvider getLocalDataProvider(UUID snapshotId, UUID toResolveSnapshotId)
      throws IOException {
    return omSnapshotManager.getSnapshotLocalDataManager().getOmSnapshotLocalData(snapshotId, toResolveSnapshotId);
  }

  UncheckedAutoCloseableSupplier<OmSnapshot> getSnapshot(SnapshotInfo snapshotInfo) throws IOException {
    return omSnapshotManager.getActiveSnapshot(snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
        snapshotInfo.getName());
  }

  OMMetadataManager getActiveMetadataManager() {
    return activeMetadataManager;
  }

  @Override
  public void close() throws IOException {
    if (tmpDeltaFileLinkDir == null || Files.notExists(tmpDeltaFileLinkDir)) {
      return;
    }
    deleteDirectory(tmpDeltaFileLinkDir);
  }
}
