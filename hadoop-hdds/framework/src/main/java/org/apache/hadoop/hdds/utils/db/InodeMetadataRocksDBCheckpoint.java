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

package org.apache.hadoop.hdds.utils.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB checkpoint implementation that uses hardlinks to optimize disk space
 * for inode-based metadata checkpoints.
 *
 * <p>During construction, reads a hardlink mapping file and creates hardlinks
 * from checkpoint files to the checkpoint directory. When deleteSourceFiles
 * is true (v2 format), original files are deleted since they're accessible via
 * hardlinks. When false (v1 format), source files are preserved as they are real
 * paths.
 * </p>
 *
 */
public class InodeMetadataRocksDBCheckpoint implements DBCheckpoint {

  private final Path checkpointLocation;
  private final long checkpointTimestamp = System.currentTimeMillis();

  private static final Logger LOG =
      LoggerFactory.getLogger(InodeMetadataRocksDBCheckpoint.class);

  public static final String OM_HARDLINK_FILE = "hardLinkFile";

  /**
   * Creates checkpoint with deleteSourceFiles=true (v2 format).
   */
  public InodeMetadataRocksDBCheckpoint(Path checkpointLocation) throws IOException {
    this(checkpointLocation, true);
  }

  /**
   * @param checkpointLocation path to untarred checkpoint directory
   * @param deleteSourceFiles when true (v2), delete source files after creating
   *        hardlinks, when false (v1), preserve them.
   */
  public InodeMetadataRocksDBCheckpoint(Path checkpointLocation,
      boolean deleteSourceFiles) throws IOException {
    this.checkpointLocation = checkpointLocation;
    installHardLinks(deleteSourceFiles);
    moveActiveDbFilesToOmDbIfNeeded();
  }

  @Override
  public Path getCheckpointLocation() {
    return this.checkpointLocation;
  }

  @Override
  public long getCheckpointTimestamp() {
    return this.checkpointTimestamp;
  }

  @Override
  public long getLatestSequenceNumber() {
    return -1;
  }

  @Override
  public long checkpointCreationTimeTaken() {
    return 0L;
  }

  @Override
  public void cleanupCheckpoint() throws IOException {
    LOG.info("Cleaning up RocksDB checkpoint at {}",
        checkpointLocation.toString());
    FileUtils.deleteDirectory(checkpointLocation.toFile());
  }

  private void installHardLinks(boolean deleteSourceFiles) throws IOException {
    File hardLinkFile = new File(checkpointLocation.toFile(),
        OM_HARDLINK_FILE);

    if (!hardLinkFile.exists()) {
      LOG.error("Hardlink file : {} does not exist.", hardLinkFile);
      return;
    }

    // Track source files that need to be deleted after hardlink creation
    Set<Path> sourceFilesToDelete = new HashSet<>();

    // Read file and create hardlinks directly in checkpointLocation
    try (Stream<String> s = Files.lines(hardLinkFile.toPath())) {
      List<String> lines = s.collect(Collectors.toList());

      // Create hardlinks directly in checkpointLocation
      for (String l : lines) {
        String[] parts = l.split(OzoneConsts.HARDLINK_SEPARATOR);
        if (parts.length != 2) {
          LOG.warn("Skipping malformed line in hardlink file: {}", l);
          continue;
        }
        String to = parts[0];      // Destination path (relative)
        String from = parts[1];    // Source path (relative to checkpointLocation)

        Path sourcePath = checkpointLocation.resolve(from).toAbsolutePath();
        Path targetPath = checkpointLocation.resolve(to).toAbsolutePath();

        // Track source file for later deletion (only when deleteSourceFiles)
        if (deleteSourceFiles && Files.exists(sourcePath)) {
          sourceFilesToDelete.add(sourcePath);
        }

        // Make parent directory if it doesn't exist
        Path parent = targetPath.getParent();
        if (parent != null && !Files.exists(parent)) {
          Files.createDirectories(parent);
        }

        // Create hardlink directly in checkpointLocation
        Files.createLink(targetPath, sourcePath);
      }

      // Delete hardlink file
      if (!hardLinkFile.delete()) {
        throw new IOException("Failed to delete: " + hardLinkFile);
      }

      // Delete source files only when deleteSourceFiles is true (v2 format)
      if (deleteSourceFiles) {
        for (Path sourcePath : sourceFilesToDelete) {
          try {
            if (Files.isDirectory(sourcePath)) {
              FileUtils.deleteDirectory(sourcePath.toFile());
            } else {
              Files.delete(sourcePath);
            }
          } catch (IOException e) {
            LOG.warn("Failed to delete source file {}: {}", sourcePath, e.getMessage());
            // Continue with other files
          }
        }
      }
    }
  }

  /**
   * For v1 format, active DB files (SST, CURRENT, etc.) will be at checkpoint root
   * with no om.db. Create om.db and move them so installCheckpoint always sees
   * checkpointLocation/om.db and checkpointLocation/db.snapshots.
   */
  private void moveActiveDbFilesToOmDbIfNeeded() throws IOException {
    Path omDbDir = checkpointLocation.resolve(OzoneConsts.OM_DB_NAME);
    if (Files.exists(omDbDir) && Files.isDirectory(omDbDir)) {
      return;
    }
    try (Stream<Path> entries = Files.list(checkpointLocation)) {
      List<Path> toMove = entries
          .filter(p -> {
            String name = p.getFileName() != null ? p.getFileName().toString() : "";
            return p.toFile().isFile() && !name.equals(OM_HARDLINK_FILE)
                && !name.equals(HddsServerUtil.OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME);
          })
          .collect(Collectors.toList());
      if (toMove.isEmpty()) {
        return;
      }
      LOG.info("Creating om.db and moving {} active DB files for v1 format", toMove.size());
      Files.createDirectories(omDbDir);
      for (Path source : toMove) {
        Path target = omDbDir.resolve(source.getFileName());
        Files.move(source, target);
      }
    }
  }
}
