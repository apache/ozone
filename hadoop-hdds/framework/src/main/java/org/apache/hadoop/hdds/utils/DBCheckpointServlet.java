/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;

import org.apache.commons.lang3.StringUtils;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.writeDBCheckpointToStream;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;

import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.security.UserGroupInformation;
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
    }

    this.aclEnabled = omAclEnabled;
    this.admins = new OzoneAdmins(allowedAdminUsers, allowedAdminGroups);
    this.isSpnegoEnabled = isSpnegoAuthEnabled;
    lock = new Lock();
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

    DBCheckpoint checkpoint = null;

    boolean flush = false;
    String flushParam =
        request.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH);
    if (StringUtils.isNotEmpty(flushParam)) {
      flush = Boolean.parseBoolean(flushParam);
    }

    List<String> receivedSstList = new ArrayList<>();
    List<String> excludedSstList = new ArrayList<>();
    String[] sstParam = isFormData ?
        parseFormDataParameters(request) : request.getParameterValues(
        OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST);
    if (sstParam != null) {
      receivedSstList.addAll(
          Arrays.stream(sstParam)
              .filter(s -> s.endsWith(ROCKSDB_SST_SUFFIX))
              .distinct()
              .collect(Collectors.toList()));
      LOG.info("Received excluding SST {}", receivedSstList);
    }

    try (BootstrapStateHandler.Lock lock = getBootstrapStateLock().lock()) {
      checkpoint = dbStore.getCheckpoint(flush);
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
      writeDbDataToStream(checkpoint, request,
          response.getOutputStream(), receivedSstList, excludedSstList);
      Instant end = Instant.now();

      long duration = Duration.between(start, end).toMillis();
      LOG.info("Time taken to write the checkpoint to response output " +
          "stream: {} milliseconds", duration);

      LOG.info("Excluded SST {} from the latest checkpoint.",
          excludedSstList);
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

  /**
   * Parses request form data parameters.
   * @param request the HTTP servlet request
   * @return array of parsed sst form data parameters for exclusion
   */
  private static String[] parseFormDataParameters(HttpServletRequest request) {
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
      LOG.warn("Exception occured during form data parsing {}", e.getMessage());
    }

    return sstParam.size() == 0 ? null : sstParam.toArray(new String[0]);
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
   * @param excludedList  the files excluded
   *
   */
  public void writeDbDataToStream(DBCheckpoint checkpoint,
      HttpServletRequest ignoredRequest,
      OutputStream destination,
      List<String> toExcludeList,
      List<String> excludedList)
      throws IOException, InterruptedException {
    Objects.requireNonNull(toExcludeList);
    Objects.requireNonNull(excludedList);

    writeDBCheckpointToStream(checkpoint, destination,
        toExcludeList, excludedList);
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  /**
   * This lock is a no-op but can overridden by child classes.
   */
  public static class Lock extends BootstrapStateHandler.Lock {
    public Lock() {
    }

    @Override
    public BootstrapStateHandler.Lock lock()
        throws InterruptedException {
      return this;
    }

    @Override
    public void unlock() {
    }
  }
}
