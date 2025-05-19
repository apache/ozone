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

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.util.Time;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.utils.Archiver.includeFile;
import static org.apache.hadoop.hdds.utils.Archiver.tar;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;

public class OMDBCheckPointServletInodeBasedXfer  extends OMDBCheckpointServlet {

  @Override
  public void writeDbDataToStream(DBCheckpoint checkpoint, HttpServletRequest request, OutputStream destination,
      List<String> toExcludeList, List<String> excludedList, Path tmpdir) throws IOException, InterruptedException {

    // Key is the InodeID and path is the first encountered file path with this inodeID
    // This will be later used to while writing to the tar.
    Map<String, Path> copyFiles = new HashMap<>();

    try (ArchiveOutputStream<TarArchiveEntry> archiveOutputStream = tar(destination)) {
      RocksDBCheckpointDiffer differ =
          getDbStore().getRocksDBCheckpointDiffer();
      DirectoryData sstBackupDir = new DirectoryData(tmpdir,
          differ.getSSTBackupDir());
      DirectoryData compactionLogDir = new DirectoryData(tmpdir,
          differ.getCompactionLogDir());

      Set<String> sstFilesToExclude = new HashSet<>(toExcludeList);

      Map<Object, Set<Path>> hardlinkFiles = new HashMap<>();

      boolean completed = getFilesForArchive(checkpoint, copyFiles, hardlinkFiles,
          sstFilesToExclude,
          includeSnapshotData(request), excludedList,
          sstBackupDir, compactionLogDir);
      writeFilesToArchive(copyFiles, hardlinkFiles, archiveOutputStream,
          completed, checkpoint.getCheckpointLocation());

    } catch (Exception e) {
      LOG.error("got exception writing to archive " + e);
      throw e;
    }
  }

  private void writeFilesToArchive(Map<String, Path> copyFiles, Map<Object, Set<Path>> hardlinkFiles,
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
      bytesWritten += includeFile(file, entry.getKey(), archiveOutputStream);
      // Log progress every 30 seconds
      if (Time.monotonicNow() - lastLoggedTime >= 30000) {
        LOG.info("Transferred {} KB, #files {} to checkpoint tarball stream...",
            bytesWritten / (1024), filesWritten);
        lastLoggedTime = Time.monotonicNow();
      }
    }

    if (completed) {
      // TODO:
      //  1. take an OM db checkpoint ,
      //  2. hold a snapshotCache lock,
      //  3. copy remaining files
      //  4. create hardlink file
    }

  }

  boolean getFilesForArchive(DBCheckpoint checkpoint, Map<String,Path> copyFiles,
       Map<Object, Set<Path>> hardlinkFiles,
       Set<String> sstFilesToExclude,  boolean includeSnapshotData,
       List<String> excluded,
       DirectoryData sstBackupDir,
       DirectoryData compactionLogDir) throws IOException {
     maxTotalSstSize =
         OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;

    // Tarball limits are not implemented for processes that don't
    // include snapshots.  Currently, this is just for recon.
    if (!includeSnapshotData) {
      maxTotalSstSize = Long.MAX_VALUE;
    }

      AtomicLong copySize = new AtomicLong(0L);

     // Log estimated total data transferred on first request.
     if (sstFilesToExclude.isEmpty()) {
       logEstimatedTarballSize(checkpoint, includeSnapshotData);
     }

     if (includeSnapshotData) {
       Set<Path> snapshotPaths = getSnapshotDirs(checkpoint, true);
       Path snapshotDir = getSnapshotDir();
       if (!processDir(snapshotDir, copyFiles, hardlinkFiles, sstFilesToExclude, snapshotPaths, excluded, copySize,
           null)) {
         return false;
       }

       // Process the tmp sst compaction dir.
       if (!processDir(sstBackupDir.getTmpDir().toPath(), copyFiles, hardlinkFiles, sstFilesToExclude, new HashSet<>(),
           excluded, copySize, sstBackupDir.getOriginalDir().toPath())) {
         return false;
       }

       if (!processDir(compactionLogDir.getTmpDir().toPath(), copyFiles, hardlinkFiles, sstFilesToExclude,
           new HashSet<>(), excluded, copySize, compactionLogDir.getOriginalDir().toPath())) {
         return false;
       }
     }

     // Get the active fs files.
     Path dir = checkpoint.getCheckpointLocation();
     if (!processDir(dir, copyFiles, hardlinkFiles, sstFilesToExclude,
         new HashSet<>(), excluded, copySize, null)) {
       return false;
     }

     return true;
   }

  private boolean processDir(Path dir, Map<String,Path> copyFiles, Map<Object, Set<Path>> hardlinkFiles,
      Set<String> sstFilesToExclude, Set<Path> snapshotPaths, List<String> excluded, AtomicLong copySize,
      Path destDir) throws IOException {

    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.collect(Collectors.toList())) {
        File f = file.toFile();
        if (f.isDirectory()) {
          // Skip any unexpected snapshot files.
          String parent = f.getParent();
          if (parent != null && parent.contains(OM_SNAPSHOT_CHECKPOINT_DIR)
              && !snapshotPaths.contains(file)) {
            LOG.debug("Skipping unneeded file: " + file);
            continue;
          }

          // Skip the real compaction log dir.
          File compactionLogDir = new File(getDbStore().
              getRocksDBCheckpointDiffer().getCompactionLogDir());
          if (f.equals(compactionLogDir)) {
            LOG.debug("Skipping compaction log dir");
            continue;
          }

          // Skip the real compaction sst backup dir.
          File sstBackupDir = new File(getDbStore().
              getRocksDBCheckpointDiffer().getSSTBackupDir());
          if (f.equals(sstBackupDir)) {
            LOG.debug("Skipping sst backup dir");
            continue;
          }
          // findbugs nonsense
          Path filename = file.getFileName();
          if (filename == null) {
            throw new IOException("file has no filename:" + file);
          }

          // Update the dest dir to point to the sub dir
          Path destSubDir = null;
          if (destDir != null) {
            destSubDir = Paths.get(destDir.toString(),
                filename.toString());
          }
          if (!processDir(file, copyFiles, hardlinkFiles, sstFilesToExclude,
              snapshotPaths, excluded, copySize, destSubDir)) {
            return false;
          }
        } else {
          long fileSize = processFile(file, copyFiles, hardlinkFiles,
              sstFilesToExclude, excluded, destDir);
          if (copySize.get() + fileSize > maxTotalSstSize) {
            return false;
          } else {
            copySize.addAndGet(fileSize);
          }
        }
      }
    }
    return true;
  }

  private long processFile(Path file, Map<String,Path> copyFiles, Map<Object, Set<Path>> hardlinkFiles,
      Set<String> sstFilesToExclude, List<String> excluded, Path destDir) throws IOException{
    long fileSize = 0;
    Path destFile = file;

    // findbugs nonsense
    Path fileNamePath = file.getFileName();
    String fileInode = OmSnapshotUtils.getINode(file).toString();
    if (fileNamePath == null) {
      throw new IOException("file has no filename:" + file);
    }
    String fileName = fileNamePath.toString();
    // if the dest dir is not null then the file needs to be copied/linked
    // to the dest dir on the follower.
    if (destDir != null) {
      destFile = Paths.get(destDir.toString(), fileName);
    }
    // If same as existing excluded file, add a link for it.
    if (sstFilesToExclude.contains(fileInode) || copyFiles.containsKey(fileInode)) {
      hardlinkFiles.getOrDefault(fileInode, new HashSet<>()).add(destFile);
    } else {
      copyFiles.put(fileInode, destFile);
      hardlinkFiles.getOrDefault(fileInode, new HashSet<>()).add(destFile);
      fileSize = Files.size(file);
    }

    if (sstFilesToExclude.contains(fileInode)) {
      excluded.add(fileInode);
    }
    return fileSize;
  }

}
