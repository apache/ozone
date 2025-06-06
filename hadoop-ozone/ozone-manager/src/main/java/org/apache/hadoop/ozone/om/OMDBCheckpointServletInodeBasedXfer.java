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

import static org.apache.hadoop.hdds.utils.Archiver.*;
import static org.apache.hadoop.ozone.OzoneConsts.*;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_PREFIX;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.file.PathFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class OMDBCheckpointServletInodeBasedXfer extends DBCheckpointServlet {

  protected static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServletInodeBasedXfer.class);
  private static final long serialVersionUID = 1L;
  private static final PathFilter SST_FILE_FILTER =
      new SuffixFileFilter(ROCKSDB_SST_SUFFIX, IOCase.INSENSITIVE);

  @Override
  public void init() throws ServletException {
    OzoneManager om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);

    if (om == null) {
      LOG.error("Unable to initialize OMDBCheckpointServlet. OM is null");
      return;
    }

    OzoneConfiguration conf = getConf();
    // Only Ozone Admins and Recon are allowed
    Collection<String> allowedUsers =
        new LinkedHashSet<>(om.getOmAdminUsernames());
    Collection<String> allowedGroups = om.getOmAdminGroups();
    ReconConfig reconConfig = conf.getObject(ReconConfig.class);
    String reconPrincipal = reconConfig.getKerberosPrincipal();
    if (!reconPrincipal.isEmpty()) {
      UserGroupInformation ugi =
          UserGroupInformation.createRemoteUser(reconPrincipal);
      allowedUsers.add(ugi.getShortUserName());
    }

    initialize(om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        allowedUsers,
        allowedGroups,
        om.isSpnegoEnabled());
  }


  protected static Path getMetaDirPath(Path checkpointLocation) {
    // This check is done to take care of findbugs else below getParent()
    // should not be null.
    Path locationParent = checkpointLocation.getParent();
    if (null == locationParent) {
      throw new RuntimeException(
          "checkpoint location's immediate parent is null.");
    }
    Path parent = locationParent.getParent();
    if (null == parent) {
      throw new RuntimeException(
          "checkpoint location's path is invalid and could not be verified.");
    }
    return parent;
  }

  // Returns value of http request parameter.
  protected static boolean includeSnapshotData(HttpServletRequest request) {
    String includeParam =
        request.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA);
    return Boolean.parseBoolean(includeParam);
  }

  @Override
  protected void processMetadataSnapshotRequest(HttpServletRequest request, HttpServletResponse response,
      boolean isFormData, DBCheckpoint checkpoint, boolean flush) {
    List<String> excludedSstList = new ArrayList<>();
    String[] sstParam = isFormData ?
        parseFormDataParameters(request) : request.getParameterValues(
        OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST);
    Set<String> receivedSstFiles = fetchSstFilesReceived(sstParam);
    Path tmpdir = null;
    try (BootstrapStateHandler.Lock lock = getBootstrapStateLock().lock()) {
      tmpdir = Files.createTempDirectory(getBootstrapTempData().toPath(),
          "bootstrap-data-");
      String tarName = "om.data-" + System.currentTimeMillis() + ".tar";
      response.setContentType("application/x-tar");
      response.setHeader("Content-Disposition", "attachment; filename=\"" + tarName + "\"");
      Instant start = Instant.now();
      writeDbDataToStream(request, response.getOutputStream(), receivedSstFiles, tmpdir);
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("Time taken to write the checkpoint to response output " +
          "stream: {} milliseconds", duration);
      logSstFileList(excludedSstList,
          "Excluded {} SST files from the latest checkpoint{}: {}", 5);
    } catch (Exception e){
      LOG.error(
          "Unable to process metadata snapshot request. ", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } finally {
      try {
        if (tmpdir != null) {
          FileUtils.deleteDirectory(tmpdir.toFile());
        }
      } catch (IOException e) {
        LOG.error("unable to delete: " + tmpdir);
      }
    }
    super.processMetadataSnapshotRequest(request, response, isFormData, checkpoint, flush);
  }


  public void writeDbDataToStream(HttpServletRequest request, OutputStream destination,
      Set<String> sstFilesToExclude, Path tmpdir) throws IOException, InterruptedException {
    DBCheckpoint checkpoint = null;
    // Key is the InodeID and the first encountered file path with this inodeID
    Map<String, Path> copyFiles = new HashMap<>();
    OzoneManager om = (OzoneManager) getServletContext().getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
    OMMetadataManager omMetadataManager = om.getMetadataManager();
    boolean includeSnapshotData = includeSnapshotData(request);
    AtomicLong maxTotalSstSize = new AtomicLong(getConf().getLong(OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY,
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT));

    if (!includeSnapshotData) {
      maxTotalSstSize.set(Long.MAX_VALUE);
    }

    boolean shouldContinue = true;

    Set<Path> allPathsToVisit = new HashSet<>();
    try (ArchiveOutputStream<TarArchiveEntry> archiveOutputStream = tar(destination)) {
      if (includeSnapshotData) {
        Set<Path> snapshotDbPaths = collectSnapshotPaths(omMetadataManager);
        // Process each snapshot db path and write it to archive
        for (Path snapshotDbPath : snapshotDbPaths) {
          if (!shouldContinue) {
            break;
          }
          shouldContinue = writeDBToArchive(copyFiles, sstFilesToExclude, snapshotDbPath,
              maxTotalSstSize, archiveOutputStream, tmpdir);
        }

        RocksDBCheckpointDiffer differ = getDbStore().getRocksDBCheckpointDiffer();
        Path sstBackupDir = new File(differ.getSSTBackupDir()).toPath();
        Path compactionLogDir = new File(differ.getCompactionLogDir()).toPath();

        if (shouldContinue) {
          shouldContinue = writeDBToArchive(copyFiles, sstFilesToExclude, sstBackupDir,
              maxTotalSstSize, archiveOutputStream,  tmpdir);
        }

        if (shouldContinue) {
          shouldContinue = writeDBToArchive(copyFiles, sstFilesToExclude, compactionLogDir,
              maxTotalSstSize, archiveOutputStream,  tmpdir);
        }
        // Add paths of snapshot databases, SST backups, and compaction logs to visit list
        allPathsToVisit.addAll(snapshotDbPaths);
        allPathsToVisit.add(sstBackupDir);
        allPathsToVisit.add(compactionLogDir);
      }

      if (shouldContinue) {
        // we finished transferring files from snapshot DB's by now and
        // this is the last step where we transfer the active om.db contents
        checkpoint = getCheckpoint(tmpdir,true);
        // unlimited files as we want the Active DB contents to be transferred in a single batch
        maxTotalSstSize.set(Long.MAX_VALUE);
        Path checkpointDir = checkpoint.getCheckpointLocation();
        writeDBToArchive(copyFiles, sstFilesToExclude, checkpointDir,
            maxTotalSstSize, archiveOutputStream, tmpdir);
        allPathsToVisit.add(checkpointDir);
        generateAndWriteHardlinkFile(checkpointDir, allPathsToVisit, archiveOutputStream);
      }

    } catch (Exception e) {
      LOG.error("got exception writing to archive " + e);
      throw e;
    } finally {
      cleanupCheckpoint(checkpoint);
    }
  }

  private static void cleanupCheckpoint(DBCheckpoint checkpoint) {
    if (checkpoint != null) {
      try {
        checkpoint.cleanupCheckpoint();
      } catch (IOException e) {
        LOG.error("Error trying to clean checkpoint at {} .",
            checkpoint.getCheckpointLocation().toString());
      }
    }
  }

  /**
   * Generates a file containing hardlink information and writes it to the archive.
   *
   * @param checkpointDir Directory containing the checkpoint
   * @param allPathsToVisit Set of all paths to visit for hardlink information
   * @param archiveOutputStream Archive output stream
   * @throws IOException if an I/O error occurs
   */
  private static void generateAndWriteHardlinkFile(Path checkpointDir, Set<Path> allPathsToVisit,
      ArchiveOutputStream<TarArchiveEntry> archiveOutputStream) throws IOException {
    Path data = Files.createTempFile(DATA_PREFIX, DATA_SUFFIX);
    Path metaDirPath = getMetaDirPath(checkpointDir);
    int truncateLength = metaDirPath.toString().length() + 1;
    StringBuilder sb = new StringBuilder();

    for (Path path : allPathsToVisit) {
      try (Stream<Path> files = Files.walk(path)) {
        Iterable<Path> iterable = files::iterator;
        Iterator<Path> iterator = iterable.iterator();
        for (; iterator.hasNext();) {
          Path p = iterator.next();
          String fileId = OmSnapshotUtils.getInodeAndMtime(p);
          String relativePath = p.toString().substring(truncateLength);
          sb.append(fileId).append("\t").append(relativePath).append("\n");
        }
      }
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8));
    includeFile(data.toFile(), OmSnapshotManager.OM_HARDLINK_FILE, archiveOutputStream);
  }

  /**
   * Gets the configuration from the OzoneManager context.
   *
   * @return OzoneConfiguration instance
   */
  private OzoneConfiguration getConf() {
    return ((OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .getConfiguration();
  }

  /**
   * Collects paths to all snapshot databases.
   *
   * @param omMetadataManager OMMetadataManager instance
   * @return Set of paths to snapshot databases
   * @throws IOException if an I/O error occurs
   */
  Set<Path> collectSnapshotPaths(OMMetadataManager omMetadataManager) throws IOException {
    Set<Path> snapshotDbPaths = new HashSet<>();
    SnapshotChainManager snapshotChainManager = new SnapshotChainManager(omMetadataManager);
    for (SnapshotChainInfo snapInfo : snapshotChainManager.getGlobalSnapshotChain().values()) {
      String snapshotDir =
          OmSnapshotManager.getSnapshotPath(getConf(), SnapshotInfo.getCheckpointDirName(snapInfo.getSnapshotId()));
      Path path = Paths.get(snapshotDir);
      snapshotDbPaths.add(path);
    }
    return snapshotDbPaths;
  }

  /**
   * Writes database files to the archive, handling deduplication based on inode IDs.
   *
   * @param copyFiles Map of inode IDs to file paths that have already been processed
   * @param sstFilesToExclude Set of SST file IDs to exclude from the archive
   * @param dbDir Directory containing database files to archive
   * @param maxTotalSstSize Maximum total size of SST files to include
   * @param archiveOutputStream Archive output stream
   * @param tmpDir Temporary directory for processing
   * @return true if processing should continue, false if size limit reached
   * @throws IOException if an I/O error occurs
   */
  boolean writeDBToArchive(Map<String, Path> copyFiles,
      Set<String> sstFilesToExclude, Path dbDir, AtomicLong maxTotalSstSize,
      ArchiveOutputStream<TarArchiveEntry> archiveOutputStream, Path tmpDir) throws IOException {
    try (Stream<Path> files = Files.list(dbDir)) {
      Iterable<Path> iterable = files::iterator;
      for (Path dbFile : iterable) {
        if (Files.isDirectory(dbFile)) {
          writeDBToArchive(copyFiles, sstFilesToExclude, dbFile, maxTotalSstSize,
              archiveOutputStream, tmpDir);
        } else {
          String fileId = OmSnapshotUtils.getInodeAndMtime(dbFile);
          if (!sstFilesToExclude.contains(fileId)) {
            long fileSize = Files.size(dbFile);
            if (maxTotalSstSize.get() - fileSize <= 0) {
              return false;
            }
            linkAndIncludeFile(dbFile.toFile(), fileId, archiveOutputStream, tmpDir);
            maxTotalSstSize.addAndGet(-fileSize);
            sstFilesToExclude.add(fileId);
          }
        }
      }
    }
    return true;
  }
}
