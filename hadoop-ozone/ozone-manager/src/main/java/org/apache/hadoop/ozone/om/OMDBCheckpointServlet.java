/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the current checkpoint Snapshot of the OM DB. (tar.gz)
 *
 * When Ozone ACL is enabled (`ozone.acl.enabled`=`true`), only users/principals
 * configured in `ozone.administrator` (along with the user that starts OM,
 * which automatically becomes an Ozone administrator but not necessarily in
 * the config) are allowed to access this endpoint.
 *
 * If Kerberos is enabled, the principal should be appended to
 * `ozone.administrator`, e.g. `scm/scm@EXAMPLE.COM`
 * If Kerberos is not enabled, simply append the login user name to
 * `ozone.administrator`, e.g. `scm`
 */
public class OMDBCheckpointServlet extends HttpServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;

  private transient OzoneManager om;
  private transient DBStore omDbStore;
  private transient OMMetrics omMetrics;
  private transient DataTransferThrottler throttler = null;

  @Override
  public void init() throws ServletException {

    om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);

    if (om == null) {
      LOG.error("Unable to initialize OMDBCheckpointServlet. OM is null");
      return;
    }

    omDbStore = om.getMetadataManager().getStore();
    omMetrics = om.getMetrics();

    OzoneConfiguration configuration = om.getConfiguration();
    long transferBandwidth = configuration.getLongBytes(
        OMConfigKeys.OZONE_DB_CHECKPOINT_TRANSFER_RATE_KEY,
        OMConfigKeys.OZONE_DB_CHECKPOINT_TRANSFER_RATE_DEFAULT);

    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
  }

  private boolean hasPermission(String username) {
    // Check ACL for dbCheckpoint only when global Ozone ACL is enabled
    if (om.getAclsEnabled()) {
      // Only Ozone admins are allowed
      try {
        Collection<String> admins = om.getOzoneAdmins(om.getConfiguration());
        if (admins.contains(OZONE_ADMINISTRATORS_WILDCARD) ||
            admins.contains(username)) {
          return true;
        }
      } catch (IOException e) {
        LOG.warn("Error checking permission: {}", e.getMessage());
      }
      return false;
    } else {
      return true;
    }
  }

  /**
   * Process a GET request for the Ozone Manager DB checkpoint snapshot.
   *
   * @param request  The servlet request we are processing
   * @param response The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {

    LOG.info("Received request to obtain OM DB checkpoint snapshot");
    if (omDbStore == null) {
      LOG.error(
          "Unable to process metadata snapshot request. DB Store is null");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      return;
    }

    // Check ACL for dbCheckpoint only when global Ozone ACL is enable
    if (om.getAclsEnabled()) {
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
        if (!hasPermission(userPrincipalName)) {
          LOG.error("Permission denied: User principal '{}' does not have"
              + " access to /dbCheckpoint.\nThis can happen when Ozone Manager"
              + " is started with a different user.\nPlease append '{}' to OM"
              + " 'ozone.administrators' config and restart OM to grant current"
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
    try {

      boolean flush = false;
      String flushParam =
          request.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH);
      if (StringUtils.isNotEmpty(flushParam)) {
        flush = Boolean.valueOf(flushParam);
      }

      checkpoint = omDbStore.getCheckpoint(flush);
      if (checkpoint == null || checkpoint.getCheckpointLocation() == null) {
        LOG.error("Unable to process metadata snapshot request. " +
            "Checkpoint request returned null.");
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        return;
      }
      omMetrics.setLastCheckpointCreationTimeTaken(
          checkpoint.checkpointCreationTimeTaken());

      Path file = checkpoint.getCheckpointLocation().getFileName();
      if (file == null) {
        return;
      }
      response.setContentType("application/x-tgz");
      response.setHeader("Content-Disposition",
          "attachment; filename=\"" +
               file.toString() + ".tgz\"");

      Instant start = Instant.now();
      writeOmDBCheckpointToStream(checkpoint,
          response.getOutputStream());
      Instant end = Instant.now();

      long duration = Duration.between(start, end).toMillis();
      LOG.info("Time taken to write the checkpoint to response output " +
          "stream: {} milliseconds", duration);
      omMetrics.setLastCheckpointStreamingTimeTaken(duration);
      omMetrics.incNumCheckpoints();
    } catch (Exception e) {
      LOG.error(
          "Unable to process metadata snapshot request. ", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      omMetrics.incNumCheckpointFails();
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
   * Write OM DB Checkpoint to an output stream as a compressed file (tgz).
   *
   * @param checkpoint  checkpoint file
   * @param destination desination output stream.
   * @throws IOException
   */
  public static void writeOmDBCheckpointToStream(DBCheckpoint checkpoint,
      OutputStream destination)
      throws IOException {

    try (CompressorOutputStream gzippedOut = new CompressorStreamFactory()
        .createCompressorOutputStream(CompressorStreamFactory.GZIP,
            destination)) {

      try (ArchiveOutputStream archiveOutputStream =
          new TarArchiveOutputStream(gzippedOut)) {

        Path checkpointPath = checkpoint.getCheckpointLocation();
        try (Stream<Path> files = Files.list(checkpointPath)) {
          for (Path path : files.collect(Collectors.toList())) {
            if (path != null) {
              Path fileName = path.getFileName();
              if (fileName != null) {
                includeFile(path.toFile(), fileName.toString(),
                    archiveOutputStream);
              }
            }
          }
        }
      }
    } catch (CompressorException e) {
      throw new IOException(
          "Can't compress the checkpoint: " +
              checkpoint.getCheckpointLocation(), e);
    }
  }

  private static void includeFile(File file, String entryName,
      ArchiveOutputStream archiveOutputStream)
      throws IOException {
    ArchiveEntry archiveEntry =
        archiveOutputStream.createArchiveEntry(file, entryName);
    archiveOutputStream.putArchiveEntry(archiveEntry);
    try (FileInputStream fis = new FileInputStream(file)) {
      IOUtils.copy(fis, archiveOutputStream);
    }
    archiveOutputStream.closeArchiveEntry();
  }
}
