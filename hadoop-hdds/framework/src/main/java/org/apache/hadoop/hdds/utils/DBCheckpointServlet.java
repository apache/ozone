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

import static org.apache.hadoop.hdds.utils.HddsServerUtil.writeDBCheckpointToStream;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the current checkpoint Snapshot of the OM/SCM DB. (tar)
 */
public class DBCheckpointServlet extends HttpServlet
    implements BootstrapStateHandler {

  private static final String FIELD_NAME =
      OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST + "[]";
  private static final Logger LOG =
      LoggerFactory.getLogger(DBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;

  private transient DBStore dbStore;
  private transient DBCheckpointMetrics dbMetrics;

  private boolean aclEnabled;
  private boolean isSpnegoEnabled;
  private transient OzoneAdmins admins;
  private transient BootstrapStateHandler.Lock lock;
  private transient File bootstrapTempData;

  public void initialize(DBStore store, DBCheckpointMetrics metrics,
                         boolean omAclEnabled,
                         Collection<String> allowedAdminUsers,
                         Collection<String> allowedAdminGroups,
                         boolean isSpnegoAuthEnabled)
      throws ServletException {

    dbStore = store;
    dbMetrics = metrics;
    if (dbStore == null) {
      LOG.error(
          "Unable to set metadata snapshot request. DB Store is null");
      throw new ServletException("DB Store is null");
    }

    this.aclEnabled = omAclEnabled;
    this.admins = new OzoneAdmins(allowedAdminUsers, allowedAdminGroups);
    this.isSpnegoEnabled = isSpnegoAuthEnabled;
    lock = new NoOpLock();

    // Create a directory for temp bootstrap data
    File dbLocation = dbStore.getDbLocation();
    if (dbLocation == null) {
      throw new NullPointerException("dblocation null");
    }
    String tempData = dbLocation.getParent();
    if (tempData == null) {
      throw new NullPointerException("tempData dir is null");
    }
    bootstrapTempData = Paths.get(tempData,
        "temp-bootstrap-data").toFile();
    if (bootstrapTempData.exists()) {
      try {
        FileUtils.cleanDirectory(bootstrapTempData);
      } catch (IOException e) {
        LOG.error("Failed to clean-up: {} dir.", bootstrapTempData);
        throw new ServletException("Failed to clean-up: " + bootstrapTempData);
      }
    }
    if (!bootstrapTempData.exists() &&
        !bootstrapTempData.mkdirs()) {
      throw new ServletException("Failed to make:" + bootstrapTempData);
    }
  }

  public File getBootstrapTempData() {
    return bootstrapTempData;
  }

  private boolean hasPermission(UserGroupInformation user) {
    // Check ACL for dbCheckpoint only when global Ozone ACL and SPNEGO is
    // enabled
    if (aclEnabled && isSpnegoEnabled) {
      return admins.isAdmin(user);
    } else {
      return true;
    }
  }

  protected static void logSstFileList(Collection<String> sstList, String msg, int sampleSize) {
    int count = sstList.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug(msg, count, "", sstList);
    } else if (count > sampleSize) {
      List<String> sample = sstList.stream().limit(sampleSize).collect(Collectors.toList());
      LOG.info(msg, count, ", sample", sample);
    } else {
      LOG.info(msg, count, "", sstList);
    }
  }

  /**
   * Generates Snapshot checkpoint as tar ball.
   * @param request the HTTP servlet request
   * @param response the HTTP servlet response
   * @param isFormData indicator whether request is form data
   */
  private void generateSnapshotCheckpoint(HttpServletRequest request,
      HttpServletResponse response, boolean isFormData) {
    if (dbStore == null) {
      LOG.error(
          "Unable to process metadata snapshot request. DB Store is null");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      return;
    }

    // Check ACL for dbCheckpoint only when global Ozone ACL is enabled
    if (aclEnabled) {
      final java.security.Principal userPrincipal = request.getUserPrincipal();
      if (userPrincipal == null) {
        final String remoteUser = request.getRemoteUser();
        LOG.error("Permission denied: Unauthorized access to /dbCheckpoint,"
                + " no user principal found. Current login user is {}.",
            remoteUser != null ? "'" + remoteUser + "'" : "UNKNOWN");
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        return;
      } else {
        final String userPrincipalName = userPrincipal.getName();
        UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(userPrincipalName);
        if (!hasPermission(ugi)) {
          LOG.error("Permission denied: User principal '{}' does not have"
                  + " access to /dbCheckpoint.\nThis can happen when Ozone"
                  + " Manager is started with a different user.\n"
                  + " Please append '{}' to OM 'ozone.administrators'"
                  + " config and restart OM to grant current"
                  + " user access to this endpoint.",
              userPrincipalName, userPrincipalName);
          response.setStatus(HttpServletResponse.SC_FORBIDDEN);
          return;
        }
        LOG.debug("Granted user principal '{}' access to /dbCheckpoint.",
            userPrincipalName);
      }
    }

    boolean flush = false;
    String flushParam =
        request.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH);
    if (StringUtils.isNotEmpty(flushParam)) {
      flush = Boolean.parseBoolean(flushParam);
    }

    processMetadataSnapshotRequest(request, response, isFormData, flush);
  }

  @VisibleForTesting
  public void processMetadataSnapshotRequest(HttpServletRequest request, HttpServletResponse response,
      boolean isFormData, boolean flush) {
    List<String> excludedSstList = new ArrayList<>();
    String[] sstParam = isFormData ?
        parseFormDataParameters(request) : request.getParameterValues(
        OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST);
    Set<String> receivedSstFiles = extractSstFilesToExclude(sstParam);
    DBCheckpoint checkpoint = null;
    Path tmpdir = null;
    try (UncheckedAutoCloseable lock = getBootstrapStateLock().acquireWriteLock()) {
      tmpdir = Files.createTempDirectory(bootstrapTempData.toPath(),
          "bootstrap-data-");
      checkpoint = getCheckpoint(tmpdir, flush);
      if (checkpoint == null || checkpoint.getCheckpointLocation() == null) {
        LOG.error("Unable to process metadata snapshot request. " +
            "Checkpoint request returned null.");
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        return;
      }
      dbMetrics.setLastCheckpointCreationTimeTaken(
          checkpoint.checkpointCreationTimeTaken());

      Path file = checkpoint.getCheckpointLocation().getFileName();
      if (file == null) {
        return;
      }
      response.setContentType("application/x-tar");
      response.setHeader("Content-Disposition",
          "attachment; filename=\"" +
               file + ".tar\"");

      Instant start = Instant.now();
      writeDbDataToStream(checkpoint, request, response.getOutputStream(),
          receivedSstFiles, tmpdir);
      Instant end = Instant.now();

      long duration = Duration.between(start, end).toMillis();
      LOG.info("Time taken to write the checkpoint to response output " +
          "stream: {} milliseconds", duration);
      logSstFileList(excludedSstList,
          "Excluded {} SST files from the latest checkpoint{}: {}", 5);
      if (!excludedSstList.isEmpty()) {
        dbMetrics.incNumIncrementalCheckpoint();
      }
      dbMetrics.setLastCheckpointStreamingNumSSTExcluded(
          excludedSstList.size());
      dbMetrics.setLastCheckpointStreamingTimeTaken(duration);
      dbMetrics.incNumCheckpoints();
    } catch (Exception e) {
      LOG.error(
          "Unable to process metadata snapshot request. ", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      dbMetrics.incNumCheckpointFails();
    } finally {
      try {
        if (tmpdir != null) {
          FileUtils.deleteDirectory(tmpdir.toFile());
        }
      } catch (IOException e) {
        LOG.error("unable to delete: " + tmpdir);
      }

      if (checkpoint != null) {
        try {
          checkpoint.cleanupCheckpoint();
        } catch (IOException e) {
          LOG.error("Error trying to clean checkpoint at {} .",
              checkpoint.getCheckpointLocation().toString());
        }
      }
    }
  }

  protected static Set<String> extractSstFilesToExclude(String[] filesInExclusionParam) {
    Set<String> sstFilesToExclude = new HashSet<>();
    if (filesInExclusionParam != null) {
      sstFilesToExclude.addAll(
          Arrays.stream(filesInExclusionParam).filter(s -> s.endsWith(ROCKSDB_SST_SUFFIX))
              .distinct().collect(Collectors.toList()));
      logSstFileList(sstFilesToExclude, "Received list of {} SST files to be excluded{}: {}", 5);
    }
    return sstFilesToExclude;
  }

  protected static Set<String> extractFilesToExclude(String[] sstParam) {
    Set<String> receivedSstFiles = new HashSet<>();
    if (sstParam != null) {
      receivedSstFiles.addAll(
          Arrays.stream(sstParam).distinct().collect(Collectors.toList()));
      logSstFileList(receivedSstFiles, "Received list of {} SST files to be excluded{}: {}", 5);
    }
    return receivedSstFiles;
  }

  public DBCheckpoint getCheckpoint(Path ignoredTmpdir, boolean flush)
      throws IOException {
    return dbStore.getCheckpoint(flush);
  }

  /**
   * Parses request form data parameters.
   * @param request the HTTP servlet request
   * @return array of parsed sst form data parameters for exclusion
   */
  protected static String[] parseFormDataParameters(HttpServletRequest request) {
    ServletFileUpload upload = new ServletFileUpload();
    List<String> sstParam = new ArrayList<>();

    try {
      FileItemIterator iter = upload.getItemIterator(request);
      while (iter.hasNext()) {
        FileItemStream item = iter.next();
        if (!item.isFormField() || !FIELD_NAME.equals(item.getFieldName())) {
          continue;
        }

        sstParam.add(Streams.asString(item.openStream()));
      }
    } catch (Exception e) {
      LOG.warn("Exception occurred during form data parsing {}", e.getMessage());
    }

    return sstParam.isEmpty() ? null : sstParam.toArray(new String[0]);
  }

  /**
   * Process a GET request for the DB checkpoint snapshot.
   *
   * @param request  The servlet request we are processing
   * @param response The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    LOG.info("Received GET request to obtain DB checkpoint snapshot");

    generateSnapshotCheckpoint(request, response, false);
  }

  /**
   * Process a POST request for the DB checkpoint snapshot.
   *
   * @param request  The servlet request we are processing
   * @param response The servlet response we are creating
   */
  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response) {
    LOG.info("Received POST request to obtain DB checkpoint snapshot");

    if (!ServletFileUpload.isMultipartContent(request)) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    generateSnapshotCheckpoint(request, response, true);
  }

  /**
   * Write checkpoint to the stream.
   *
   * @param checkpoint The checkpoint to be written.
   * @param ignoredRequest The httpRequest which generated this checkpoint.
   *        (Parameter is ignored in this class but used in child classes).
   * @param destination The stream to write to.
   * @param toExcludeList the files to be excluded
   *
   */
  public void writeDbDataToStream(DBCheckpoint checkpoint,
      HttpServletRequest ignoredRequest,
      OutputStream destination,
      Set<String> toExcludeList,
      Path tmpdir)
      throws IOException, InterruptedException {
    Objects.requireNonNull(toExcludeList);
    writeDBCheckpointToStream(checkpoint, destination, toExcludeList);
  }

  public DBStore getDbStore() {
    return dbStore;
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  /**
   * This lock is a no-op but can overridden by child classes.
   */
  public static class NoOpLock extends BootstrapStateHandler.Lock {
    private static final UncheckedAutoCloseable NOOP_LOCK = () -> {
    };

    public NoOpLock() {
      super((readLock) -> NOOP_LOCK);
    }

    @Override
    public UncheckedAutoCloseable acquireReadLock() {
      return NOOP_LOCK;
    }

    @Override
    public UncheckedAutoCloseable acquireWriteLock() {
      return NOOP_LOCK;
    }
  }
}
