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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.hdds.utils.IOUtils.getINode;
import static org.apache.hadoop.ozone.OzoneConsts.HARDLINK_SEPARATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone Manager Snapshot Utilities.
 */
public final class OmSnapshotUtils {

  public static final String DATA_PREFIX = "data";
  public static final String DATA_SUFFIX = "txt";
  public static final String PATH_SEPARATOR = "/";

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
   * Returns a string combining the inode (fileKey) and the last modification time (mtime) of the given file.
   * <p>
   * The returned string is formatted as "{inode}-{mtime}", where:
   * <ul>
   *   <li>{@code inode} is the unique file key obtained from the file system, typically representing
   *   the inode on POSIX systems</li>
   *   <li>{@code mtime} is the last modified time of the file in milliseconds since the epoch</li>
   * </ul>
   *
   * @param file the {@link Path} to the file whose inode and modification time are to be retrieved
   * @return a string in the format "{inode}-{mtime}"
   * @throws IOException if an I/O error occurs
   */
  public static String getFileInodeAndLastModifiedTimeString(Path file) throws IOException {
    Object inode = getINode(file);
    FileTime mTime = Files.getLastModifiedTime(file);
    return String.format("%s-%s", inode, mTime.toMillis());
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
   * @param hardLinkFiles  - Map of link-&gt;file paths.
   * @return Path to the file of links created.
   */
  public static Path createHardLinkList(int truncateLength,
                                        Map<Path, Path> hardLinkFiles)
      throws IOException {
    Path data = Files.createTempFile(DATA_PREFIX, DATA_SUFFIX);
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
      sb.append(truncateFileName(truncateLength, entry.getKey())).append(HARDLINK_SEPARATOR)
          .append(fixedFile).append('\n');
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8));
    return data;
  }

  /**
   * Link each of the files in oldDir to newDir.
   *
   * @param oldDir The dir to create links from.
   * @param newDir The dir to create links to.
   */
  public static void linkFiles(File oldDir, File newDir) throws IOException {
    int truncateLength = oldDir.toString().length() + 1;
    List<String> oldDirList;
    try (Stream<Path> files = Files.walk(oldDir.toPath())) {
      oldDirList = files.map(Path::toString).
          // Don't copy the directory itself
          filter(s -> !s.equals(oldDir.toString())).
          // Remove the old path
          map(s -> s.substring(truncateLength)).
          sorted().
          collect(Collectors.toList());
    }
    for (String s: oldDirList) {
      File oldFile = new File(oldDir, s);
      File newFile = new File(newDir, s);
      File newParent = newFile.getParentFile();
      if (!newParent.exists()) {
        if (!newParent.mkdirs()) {
          throw new IOException("Directory create fails: " + newParent);
        }
      }
      if (oldFile.isDirectory()) {
        if (!newFile.mkdirs()) {
          throw new IOException("Directory create fails: " + newFile);
        }
      } else {
        Files.createLink(newFile.toPath(), oldFile.toPath());
      }
    }
  }
}
