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

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;

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
public class OMDBCheckpointServlet extends DBCheckpointServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;
  private static final int MAX_SECONDS_TO_WAIT = 10;

  @Override
  public void init() throws ServletException {

    OzoneManager om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);

    if (om == null) {
      LOG.error("Unable to initialize OMDBCheckpointServlet. OM is null");
      return;
    }

    OzoneConfiguration conf = om.getConfiguration();
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

  private void getFilesFromCheckpoint(DBCheckpoint checkpoint,
                                      Map<Object, Path> copyFiles)
      throws IOException {
    Path dir = checkpoint.getCheckpointLocation();

    try (Stream<Path> files = Files.list(dir)) {
      for(Path file : files.collect(Collectors.toList())) {
        // get the inode
        Object key = Files.readAttributes(
            file, BasicFileAttributes.class).fileKey();
        copyFiles.put(key, file);
      }
    }
  }
  private List<Path> getSnapshotDirs(DBCheckpoint checkpoint)
      throws IOException {
    ArrayList<Path> list = new ArrayList<>();

    // get snapshotInfo entries
    OzoneConfiguration conf = ((OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .getConfiguration();

    OmMetadataManagerImpl checkpointMetadataManager =
        OmMetadataManagerImpl.createCheckpointMetadataManager(
            conf, checkpoint);
    try(TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
        iterator = checkpointMetadataManager
        .getSnapshotInfoTable().iterator()) {

      // add each entries directory to the list
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> entry = iterator.next();
        Path path = Paths.get(OMStorage.getOmDbDir(conf) +
            OM_KEY_PREFIX + OM_SNAPSHOT_DIR + OM_KEY_PREFIX +
            OM_DB_NAME + entry.getValue().getCheckpointDirName());
        list.add(path);
      }
    }
    return list;
  }

  private void waitForDirToExist(Path dir)
      throws IOException, InterruptedException {
    int count = 0;
    while (!dir.toFile().exists() && count < MAX_SECONDS_TO_WAIT) {
      Thread.sleep(1000);
      count++;
    }
    if (count == MAX_SECONDS_TO_WAIT) {
      throw new IOException("snapshot dir doesn't exist: " + dir);
    }
  }
  private void processDir(Path dir, Map<Object,Path> copyFiles,
                          Map<Path, Path> hardLinkFiles)
      throws IOException, InterruptedException {
    waitForDirToExist(dir);
    try (Stream<Path> files = Files.list(dir)) {
      for(Path file : files.collect(Collectors.toList())) {
        // get the inode
        Object key = Files.readAttributes(
            file, BasicFileAttributes.class).fileKey();
        if (copyFiles.containsKey(key)) {
          hardLinkFiles.put(file, copyFiles.get(key));
        } else {
          copyFiles.put(key, file);
        }
      }
    }

  }

  public void writeArchiveToStream(DBCheckpoint checkpoint, OutputStream destination)
      throws IOException, InterruptedException, CompressorException {
    HashMap<Object, Path> copyFiles = new HashMap<>();
    HashMap<Path, Path> hardLinkFiles = new HashMap<>();

    getFilesFromCheckpoint(checkpoint, copyFiles);
    for (Path dir : getSnapshotDirs(checkpoint)) {
      processDir(dir, copyFiles, hardLinkFiles);
    }

    try (CompressorOutputStream gzippedOut = new CompressorStreamFactory()
        .createCompressorOutputStream(CompressorStreamFactory.GZIP,
            destination)) {

      try (ArchiveOutputStream archiveOutputStream =
          new TarArchiveOutputStream(gzippedOut)) {
        ((TarArchiveOutputStream)archiveOutputStream)
            .setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
        Path checkpointPath = checkpoint.getCheckpointLocation();

        for (Path file : copyFiles.values()) {
          includeFile(file.toFile(), file.getFileName().toString(),
              archiveOutputStream);
        }
        Path hardLinkFile = createHardLinkList(hardLinkFiles);
        includeFile(hardLinkFile.toFile(),
            Paths.get(checkpointPath.toString(),
                "hardLinkFile").toString(),
            archiveOutputStream);
      }
    } catch (CompressorException e) {
      throw new IOException(
          "Can't compress the checkpoint: " +
              checkpoint.getCheckpointLocation(), e);
    }
  }

  private Path createHardLinkList(Map<Path, Path> hardLinkFiles)
      throws IOException {
    Path data = Files.createTempFile("hardLinkData", "txt");
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Path, Path> entry : hardLinkFiles.entrySet()) {
      sb.append(entry.getKey())
          .append("\t")
          .append(entry.getValue())
          .append("\n");
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8));
    return data;
  }

}
