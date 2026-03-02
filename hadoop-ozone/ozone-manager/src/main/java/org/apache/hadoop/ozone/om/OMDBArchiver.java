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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.utils.Archiver.includeFile;
import static org.apache.hadoop.hdds.utils.Archiver.tar;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.includeRatisSnapshotCompleteFlag;
import static org.apache.hadoop.ozone.om.OMDBCheckpointServletInodeBasedXfer.writeHardlinkFile;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for handling operations relevant to archiving the OM DB tarball.
 * Mainly maintains a map for recording the files collected from reading
 * the checkpoint and snapshot DB's. It temporarily creates hardlinks and stores
 * the link data in the map to release the bootstrap lock quickly
 * and do the actual write at the end outside the lock.
 */
public class OMDBArchiver {

  private Path tmpDir;
  private Map<String, File> filesToWriteIntoTarball;
  private final Map<String, String> hardLinkFileMap;
  private static final Logger LOG = LoggerFactory.getLogger(OMDBArchiver.class);
  private boolean completed;

  public OMDBArchiver() {
    this.tmpDir = null;
    this.filesToWriteIntoTarball = new HashMap<>();
    this.hardLinkFileMap = new HashMap<>();
    this.completed = false;
  }

  public void setTmpDir(Path tmpDir) {
    this.tmpDir = tmpDir;
  }

  public Path getTmpDir() {
    return tmpDir;
  }

  public Map<String, File> getFilesToWriteIntoTarball() {
    return filesToWriteIntoTarball;
  }

  public void recordHardLinkMapping(String absolutePath, String fileId) {
    hardLinkFileMap.put(absolutePath, fileId);
  }

  public void removeHardLinkMapping(String absolutePath) {
    hardLinkFileMap.remove(absolutePath);
  }

  public boolean isCompleted() {
    return completed;
  }

  public void setCompleted(boolean completed) {
    this.completed = completed;
  }

  /**
   * Records the given file entry into the map after taking a hardlink.
   *
   * @param file the file to create a hardlink and record into the map
   * @param entryName name of the entry corresponding to file
   * @return the file size
   * @throws IOException in case of hardlink failure
   */
  public long recordFileEntry(File file, String entryName) throws IOException {
    if (tmpDir == null) {
      throw new IllegalStateException(
          "Temporary directory not set. Call setTmpDir() before recordFileEntry().");
    }
    File link = tmpDir.resolve(entryName).toFile();
    long bytes = 0;
    try {
      Path linkPath = link.toPath();
      if (Files.exists(linkPath)) {
        // If the existing file is already a link to the same source, just reuse it.
        if (Files.isSameFile(linkPath, file.toPath())) {
          filesToWriteIntoTarball.put(entryName, link);
          return file.length();
        }
        // Otherwise, remove the stale link/entry so we can recreate it.
        Files.delete(linkPath);
      }
      Files.createLink(linkPath, file.toPath());
      filesToWriteIntoTarball.put(entryName, link);
      bytes = file.length();
    } catch (IOException ioe) {
      LOG.error("Couldn't create hardlink for file {} while including it in tarball.",
          file.getAbsolutePath(), ioe);
      throw ioe;
    }
    return bytes;
  }

  /**
   * Writes all the files captured by the map into the archive and
   * also includes the hardlinkFile and the completion marker file.
   *
   * @param conf the configuration object to obtain metadata paths
   * @param outputStream the tarball archive output stream
   * @throws IOException in case of write failure to the archive
   */
  public void writeToArchive(OzoneConfiguration conf, OutputStream outputStream)
      throws IOException {
    long bytesWritten = 0;
    long lastLoggedTime = Time.monotonicNow();
    long filesWritten = 0;
    try (ArchiveOutputStream<TarArchiveEntry> archiveOutput = tar(outputStream)) {
      for (Map.Entry<String, File> kv : filesToWriteIntoTarball.entrySet()) {
        String entryName = kv.getKey();
        File link = kv.getValue();
        try {
          bytesWritten += includeFile(link, entryName, archiveOutput);
          filesWritten++;
          if (Time.monotonicNow() - lastLoggedTime >= 30000) {
            LOG.info("Transferred {} KB, #files {} to checkpoint tarball stream...",
                bytesWritten / (1024), filesWritten);
            lastLoggedTime = Time.monotonicNow();
          }
        } catch (IOException ioe) {
          LOG.error("Failed to write file {} to checkpoint tarball archive.",
              link.getAbsolutePath(), ioe);
          throw ioe;
        } finally {
          Files.deleteIfExists(link.toPath());
        }
      }
      if (isCompleted()) {
        writeHardlinkFile(conf, hardLinkFileMap, archiveOutput);
        includeRatisSnapshotCompleteFlag(archiveOutput);
      }
    }
  }
}
