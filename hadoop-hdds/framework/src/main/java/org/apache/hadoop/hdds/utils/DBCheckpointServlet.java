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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;

/**
 * Provides the current checkpoint Snapshot of the OM/SCM DB. (tar.gz)
 */
public class DBCheckpointServlet extends HttpServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(DBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;

  private transient DBStore dbStore;
  private transient DBCheckpointMetrics dbMetrics;

  private boolean aclEnabled;
  private boolean isSpnegoEnabled;
  private Collection<String> allowedUsers;

  public void initialize(DBStore store, DBCheckpointMetrics metrics,
                         boolean omAclEnabled,
                         Collection<String> allowedAdminUsers,
                         boolean isSpnegoAuthEnabled)
      throws ServletException {

    dbStore = store;
    dbMetrics = metrics;
    if (dbStore == null) {
      LOG.error(
          "Unable to set metadata snapshot request. DB Store is null");
    }

    this.aclEnabled = omAclEnabled;
    this.allowedUsers = allowedAdminUsers;
    this.isSpnegoEnabled = isSpnegoAuthEnabled;
  }

  private boolean hasPermission(UserGroupInformation user) {
    // Check ACL for dbCheckpoint only when global Ozone ACL and SPNEGO is
    // enabled
    if (aclEnabled && isSpnegoEnabled) {
      return allowedUsers.contains(OZONE_ADMINISTRATORS_WILDCARD)
          || allowedUsers.contains(user.getShortUserName())
          || allowedUsers.contains(user.getUserName());
    } else {
      return true;
    }
  }

  /**
   * Process a GET request for the DB checkpoint snapshot.
   *
   * @param request  The servlet request we are processing
   * @param response The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
    return;
  }

  /**
   * Write DB Checkpoint to an output stream as a compressed file (tgz).
   *
   * @param checkpoint  checkpoint file
   * @param destination desination output stream.
   * @throws IOException
   */
  public static void writeDBCheckpointToStream(DBCheckpoint checkpoint,
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
