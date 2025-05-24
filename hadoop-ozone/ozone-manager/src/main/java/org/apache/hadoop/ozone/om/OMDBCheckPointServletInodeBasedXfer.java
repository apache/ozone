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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.util.Time;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;

/**
 * Specialized OMDBCheckpointServlet implementation that transfers Ozone Manager
 * database checkpoints using inode-based deduplication.
 * <p>
 * This servlet constructs checkpoint archives by examining file inodes,
 * ensuring that files with the same inode (i.e., hardlinks or duplicates)
 * are only transferred once. It maintains mappings from inode IDs to file
 * paths, manages hardlink information, and enforces snapshot and SST file
 * size constraints as needed.
 * <p>
 * This approach optimizes checkpoint streaming by reducing redundant data
 * transfer, especially in environments where RocksDB and snapshotting result
 * in multiple hardlinks to the same physical data.
 */
public class OMDBCheckPointServletInodeBasedXfer  extends OMDBCheckpointServlet {

  @Override
  public void writeDbDataToStream(DBCheckpoint checkpoint, HttpServletRequest request, OutputStream destination,
      Set<String> sstFilesToExclude, Path tmpdir) throws IOException, InterruptedException {

    // Key is the InodeID and path is the first encountered file path with this inodeID
    // This will be later used to while writing to the tar.
    Map<String, Path> copyFiles = new HashMap<>();
    Map<String, Set<Path>> hardlinkFiles = new HashMap<>();

    OzoneManager om = (OzoneManager) getServletContext().getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
    OMMetadataManager omMetadataManager = om.getMetadataManager();
    RDBStore rdbStore = (RDBStore) omMetadataManager.getStore();
    boolean includeSnapshotData = includeSnapshotData(request);

    long maxTotalSstSize = getConf().getLong(OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY,
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT);
    // Tarball limits are not implemented for processes that don't
    // include snapshots.  Currently, this is just for recon.
    if (!includeSnapshotData) {
      maxTotalSstSize = Long.MAX_VALUE;
    }

    try (ArchiveOutputStream<TarArchiveEntry> archiveOutputStream = tar(destination)) {
      Set<Path> snapshotDbPaths = new HashSet<>();
      if (includeSnapshotData) {
        RocksDBCheckpointDiffer differ = getDbStore().getRocksDBCheckpointDiffer();
        Path sstBackupDir = new File(differ.getSSTBackupDir()).toPath();
        Path compactionLogDir = new File(differ.getCompactionLogDir()).toPath();
        SnapshotChainManager snapshotChainManager = new SnapshotChainManager(omMetadataManager);
        for (SnapshotChainInfo snapInfo : snapshotChainManager.getGlobalSnapshotChain().values()) {
          String snapshotDir = OmSnapshotManager.getSnapshotPath(getConf(),
              SnapshotInfo.getCheckpointDirName(snapInfo.getSnapshotId()));
          Path path = Paths.get(snapshotDir);
          snapshotDbPaths.add(path);
        }
        snapshotDbPaths.add(sstBackupDir);
        snapshotDbPaths.add(compactionLogDir);
      }

      boolean shouldContinue = true;
      for (Path snapshotDbPath : snapshotDbPaths) {
        if (!shouldContinue) {
          break;
        }
        shouldContinue = getFilesForArchive(copyFiles, hardlinkFiles,
            sstFilesToExclude, snapshotDbPath, maxTotalSstSize);
      }

      if (shouldContinue) {
        shouldContinue = getFilesForArchive(copyFiles, hardlinkFiles,
            sstFilesToExclude, rdbStore.getDbLocation().toPath(), maxTotalSstSize);
      }

      writeFilesToArchive(copyFiles, hardlinkFiles, archiveOutputStream,
          shouldContinue, checkpoint.getCheckpointLocation());

    } catch (Exception e) {
      LOG.error("got exception writing to archive " + e);
      throw e;
    }
  }

  private OzoneConfiguration getConf() {
    return ((OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .getConfiguration();
  }

  private void writeFilesToArchive(Map<String, Path> copyFiles, Map<String, Set<Path>> hardlinkFiles,
      ArchiveOutputStream<TarArchiveEntry> archiveOutputStream, boolean completed, Path checkpointLocation)
      throws IOException {
    Map<String, Path> filteredCopyFiles = completed ? copyFiles :
        copyFiles.entrySet().stream().filter(e -> e.getValue().getFileName().toString().toLowerCase().endsWith(".sst"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Path metaDirPath = getMetaDirPath(checkpointLocation);
    long bytesWritten = 0L;
    int filesWritten = 0;
    long lastLoggedTime = Time.monotonicNow();

    // Go through each of the files to be copied and add to archive.
    for (Map.Entry<String, Path> entry : filteredCopyFiles.entrySet()) {
      Path path = entry.getValue();
      // Confirm the data is in the right place.
      if (!path.toString().startsWith(metaDirPath.toString())) {
        throw new IOException("tarball file not in metadata dir: "
            + path + ": " + metaDirPath);
      }

      File file = path.toFile();
      if (!file.isDirectory()) {
        filesWritten++;
      }
      LOG.info("Writing file = {} for inode {}", file.getAbsolutePath(), entry.getKey());
      bytesWritten += includeFile(file, entry.getKey(), archiveOutputStream);
      // Log progress every 30 seconds
      if (Time.monotonicNow() - lastLoggedTime >= 30000) {
        LOG.info("Transferred {} KB, #files {} to checkpoint tarball stream...",
            bytesWritten / (1024), filesWritten);
        lastLoggedTime = Time.monotonicNow();
      }
    }

    /*
    if (completed) {
      // TODO:
      //  1. take an OM db checkpoint ,
      //  2. hold a snapshotCache lock,
      //  3. copy remaining files
      //  4. create hardlink file
    }
    */
  }

  boolean getFilesForArchive(Map<String, Path> copyFiles, Map<String, Set<Path>> hardlinkFiles,
      Set<String> sstFilesToExclude, Path dbDir, long maxTotalSstSize) throws IOException {
    AtomicLong copySize = new AtomicLong(maxTotalSstSize);
    try (Stream<Path> files = Files.list(dbDir)) {
      Iterable<Path> iterable = files::iterator;
      for (Path dbFile : iterable) {
        if (Files.isDirectory(dbFile)) {
          getFilesForArchive(copyFiles, hardlinkFiles, sstFilesToExclude, dbFile, maxTotalSstSize);
        } else {
          String fileId = OmSnapshotUtils.getInodeAndMtime(dbFile);
          if (!sstFilesToExclude.contains(fileId)) {
            if (!copyFiles.containsKey(fileId)) {
              long fileSize = Files.size(dbFile);
              if (copySize.get() - fileSize <= 0) {
                return false;
              }
              copySize.addAndGet(-fileSize);
              copyFiles.put(fileId, dbFile);
            }
          }
          hardlinkFiles.getOrDefault(fileId, new HashSet<>()).add(dbFile);
        }
      }
    }
    return true;
  }

}
