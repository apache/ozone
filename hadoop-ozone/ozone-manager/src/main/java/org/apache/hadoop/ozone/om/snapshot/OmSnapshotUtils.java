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

package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.om.OmSnapshotManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;

/**
 * Ozone Manager Snapshot Utilities.
 */
public final class OmSnapshotUtils {

  private OmSnapshotUtils() { }

  /**
   * Get the filename without the introductory metadata directory.
   *
   * @param truncateLength Length to remove.
   * @param file           File to remove prefix from.
   * @return Truncated string.
   */
  public static String truncateFileName(int truncateLength, Path file) {
    return file.toString().substring(truncateLength);
  }

  /**
   * Get the INode for file.
   *
   * @param file File whose INode is to be retrieved.
   * @return INode for file.
   */
  @VisibleForTesting
  public static Object getINode(Path file) throws IOException {
    return Files.readAttributes(file, BasicFileAttributes.class).fileKey();
  }

  /**
   * Create file of links to add to tarball.
   * Format of entries are either:
   * dir1/fileTo fileFrom
   * for files in active db or:
   * dir1/fileTo dir2/fileFrom
   * for files in another directory, (either another snapshot dir or
   * sst compaction backup directory)
   *
   * @param truncateLength - Length of initial path to trim in file path.
   * @param hardLinkFiles  - Map of link->file paths.
   * @return Path to the file of links created.
   */
  public static Path createHardLinkList(int truncateLength,
                                        Map<Path, Path> hardLinkFiles)
      throws IOException {
    Path data = Files.createTempFile("data", "txt");
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Path, Path> entry : hardLinkFiles.entrySet()) {
      String fixedFile = truncateFileName(truncateLength, entry.getValue());
      // If this file is from the active db, strip the path.
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        Path f = Paths.get(fixedFile).getFileName();
        if (f != null) {
          fixedFile = f.toString();
        }
      }
      sb.append(truncateFileName(truncateLength, entry.getKey())).append("\t")
          .append(fixedFile).append("\n");
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8));
    return data;
  }

  /**
   * Create hard links listed in OM_HARDLINK_FILE.
   *
   * @param dbPath Path to db to have links created.
   */
  public static void createHardLinks(Path dbPath) throws IOException {
    File hardLinkFile =
        new File(dbPath.toString(), OmSnapshotManager.OM_HARDLINK_FILE);
    if (hardLinkFile.exists()) {
      // Read file.
      try (Stream<String> s = Files.lines(hardLinkFile.toPath())) {
        List<String> lines = s.collect(Collectors.toList());

        // Create a link for each line.
        for (String l : lines) {
          String from = l.split("\t")[1];
          String to = l.split("\t")[0];
          Path fullFromPath = getFullPath(dbPath, from);
          Path fullToPath = getFullPath(dbPath, to);
          Files.createLink(fullToPath, fullFromPath);
        }
        if (!hardLinkFile.delete()) {
          throw new IOException("Failed to delete: " + hardLinkFile);
        }
      }
    }
  }

  // Prepend the full path to the hard link entry entry.
  private static Path getFullPath(Path dbPath, String fileName)
      throws IOException {
    File file = new File(fileName);
    // If there is no directory then this file belongs in the db.
    if (file.getName().equals(fileName)) {
      return Paths.get(dbPath.toString(), fileName);
    }
    // Else this file belong in a directory parallel to the db.
    Path parent = dbPath.getParent();
    if (parent == null) {
      throw new IOException("Invalid database " + dbPath);
    }
    return Paths.get(parent.toString(), fileName);
  }
}
