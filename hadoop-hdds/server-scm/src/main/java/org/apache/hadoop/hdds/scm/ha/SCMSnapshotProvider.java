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

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCMSnapshotProvider downloads the latest checkpoint from the
 * leader SCM and loads the checkpoint into State Machine.
 */
public class SCMSnapshotProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMSnapshotProvider.class);

  private final File scmSnapshotDir;

  private final ConfigurationSource conf;

  private Map<String, SCMNodeDetails> peerNodesMap;

  private final CertificateClient scmCertificateClient;

  /**
   * Startup options for SCM snapshot provider.
   */
  public enum StartupOption {
    /**
     * FORMAT mode: Ratis snapshot directory should not exist.
     * Will create and initialize a new Ratis snapshot directory.
     */
    FORMAT,
    /**
     * NORMAL mode: Ratis snapshot directory should already exist.
     * Will read from existing Ratis snapshot directory.
     */
    NORMAL
  }

  /**
   * Creates SCMSnapshotProvider with default NORMAL startup option.
   * This constructor is used in most scenarios where the Ratis snapshot
   * directory is expected to already exist.
   *
   * @param conf Configuration source
   * @param peerNodes List of peer SCM nodes
   * @param scmCertificateClient Certificate client for secure communication
   */
  public SCMSnapshotProvider(ConfigurationSource conf,
      List<SCMNodeDetails> peerNodes,
      CertificateClient scmCertificateClient) {
    this(conf, peerNodes, scmCertificateClient, StartupOption.NORMAL);
  }

  /**
   * Creates SCMSnapshotProvider with specified startup option.
   *
   * @param conf Configuration source
   * @param peerNodes List of peer SCM nodes
   * @param scmCertificateClient Certificate client for secure communication
   * @param startupOption Startup mode: FORMAT (create new) or NORMAL (use existing)
   */
  public SCMSnapshotProvider(ConfigurationSource conf,
      List<SCMNodeDetails> peerNodes,
      CertificateClient scmCertificateClient,
      StartupOption startupOption) {
    LOG.info("Initializing SCM Snapshot Provider with startup option: {}",
        startupOption);
    this.conf = conf;
    this.scmCertificateClient = scmCertificateClient;

    // Get directory paths from configuration
    String scmRatisDirectory = SCMHAUtils.getSCMRatisDirectory(conf);
    String scmSnapshotDirectory = SCMHAUtils.getSCMRatisSnapshotDirectory(conf);

    if (scmRatisDirectory == null || scmRatisDirectory.isEmpty()) {
      throw new IllegalArgumentException(HddsConfigKeys.OZONE_METADATA_DIRS +
          " must be defined.");
    }

    if (scmSnapshotDirectory == null || scmSnapshotDirectory.isEmpty()) {
      throw new IllegalArgumentException("SCM Ratis snapshot directory must be defined.");
    }

    File ratisDir = new File(scmRatisDirectory);
    File snapshotDir = new File(scmSnapshotDirectory);

    // Ratis storage directory should already be created by SCMRatisServerImpl
    // or by init/bootstrap commands. SCMSnapshotProvider is NOT responsible for creating it.
    if (!ratisDir.exists()) {
      throw new IllegalStateException(
          "Ratis storage directory does not exist: " + ratisDir.getAbsolutePath()
          + ". It should have been created by SCMRatisServerImpl.initialize().");
    }

    // Handle snapshot directory based on startup option
    switch (startupOption) {
    case FORMAT:
      // FORMAT mode: snapshot directory should NOT exist
      if (snapshotDir.exists()) {
        throw new IllegalStateException(
            "Cannot format: Ratis snapshot directory already exists: "
            + snapshotDir.getAbsolutePath());
      }
      // Create new snapshot directory
      this.scmSnapshotDir = HddsUtils.createDir(scmSnapshotDirectory);
      LOG.info("Formatted: created new Ratis snapshot directory at {}",
          this.scmSnapshotDir.getAbsolutePath());
      break;

    case NORMAL:
      // NORMAL mode: snapshot directory MUST exist
      if (!snapshotDir.exists()) {
        throw new IllegalStateException(
            "Ratis snapshot directory does not exist: "
            + snapshotDir.getAbsolutePath()
            + ". Please run 'ozone scm --init' first.");
      }
      this.scmSnapshotDir = snapshotDir;
      LOG.info("Using existing Ratis snapshot directory at {}",
          this.scmSnapshotDir.getAbsolutePath());
      break;

    default:
      throw new IllegalArgumentException("Unknown startup option: " + startupOption);
    }

    // Initialize peer nodes map
    if (peerNodes != null) {
      this.peerNodesMap = new HashMap<>();
      for (SCMNodeDetails peerNode : peerNodes) {
        this.peerNodesMap.put(peerNode.getNodeId(), peerNode);
      }
    }
  }

  @VisibleForTesting
  public void setPeerNodesMap(Map<String, SCMNodeDetails> peerNodesMap) {
    this.peerNodesMap = peerNodesMap;
  }

  /**
   * Download the latest checkpoint from SCM Leader .
   * @param leaderSCMNodeID leader SCM Node ID.
   * @return the DB checkpoint (including the ratis snapshot index)
   */
  public DBCheckpoint getSCMDBSnapshot(String leaderSCMNodeID)
      throws IOException {
    String snapshotTime = Long.toString(System.currentTimeMillis());
    String snapshotFileName =
        OzoneConsts.SCM_DB_NAME + "-" + leaderSCMNodeID + "-" + snapshotTime;
    String snapshotFilePath =
        Paths.get(scmSnapshotDir.getAbsolutePath(), snapshotFileName).toFile()
            .getAbsolutePath();
    File targetFile = new File(snapshotFilePath + ".tar");


    // the downloadClient instance will be created as and when install snapshot
    // request is received. No caching of the client as it should be a very rare
    int port = peerNodesMap.get(leaderSCMNodeID).getGrpcPort();
    String host = peerNodesMap.get(leaderSCMNodeID).getInetAddress()
            .getHostAddress();

    try (SCMSnapshotDownloader downloadClient =
        new InterSCMGrpcClient(host, port, conf, scmCertificateClient)) {
      downloadClient.download(targetFile.toPath()).get();
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Rocks DB checkpoint downloading failed", e);
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }


    // Untar the checkpoint file.
    Path untarredDbDir = Paths.get(snapshotFilePath);
    FileUtil.unTar(targetFile, untarredDbDir.toFile());
    FileUtils.deleteQuietly(targetFile);

    LOG.info(
        "Successfully downloaded latest checkpoint from leader SCM: {} path {}",
        leaderSCMNodeID, untarredDbDir.toAbsolutePath());

    RocksDBCheckpoint scmCheckpoint = new RocksDBCheckpoint(untarredDbDir);
    return scmCheckpoint;
  }

  @VisibleForTesting
  public File getScmSnapshotDir() {
    return scmSnapshotDir;
  }

}
