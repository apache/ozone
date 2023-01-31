/*
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

package org.apache.hadoop.hdds.scm.ha;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.RDBSnapshotProvider;

import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCMDBSnapshotProvider downloads the latest checkpoint from the
 * leader SCM and loads the checkpoint into State Machine.
 */
public class SCMDBSnapshotProvider extends RDBSnapshotProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDBSnapshotProvider.class);

  private final ConfigurationSource conf;

  private Map<String, SCMNodeDetails> peerNodesMap;

  private final CertificateClient scmCertificateClient;

  public SCMDBSnapshotProvider(ConfigurationSource conf,
      File scmRatisSnapshotDir,
      List<SCMNodeDetails> peerNodes,
      CertificateClient scmCertificateClient) {
    super(scmRatisSnapshotDir, OzoneConsts.SCM_DB_NAME);
    LOG.info("Initializing SCM Snapshot Provider");

    this.conf = conf;
    this.scmCertificateClient = scmCertificateClient;
    // Create Ratis storage dir
    String scmRatisDirectory = SCMHAUtils.getSCMRatisDirectory(conf);

    if (scmRatisDirectory == null || scmRatisDirectory.isEmpty()) {
      throw new IllegalArgumentException(HddsConfigKeys.OZONE_METADATA_DIRS +
          " must be defined.");
    }
    HddsUtils.createDir(scmRatisDirectory);

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

  @Override
  public void downloadSnapshot(String leaderNodeID, File targetFile)
      throws IOException {
    // the downloadClient instance will be created as and when install snapshot
    // request is received. No caching of the client as it should be a very rare
    SCMNodeDetails leader = peerNodesMap.get(leaderNodeID);
    int port = leader.getGrpcPort();
    String host = leader.getInetAddress().getHostAddress();

    try (InterSCMGrpcClient downloadClient =
             new InterSCMGrpcClient(host, port, conf, scmCertificateClient)) {
      downloadClient.setSstList(HAUtils.getExistingSstFiles(getCandidateDir()));
      downloadClient.download(targetFile.toPath()).get();
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Rocks DB checkpoint downloading failed", e);
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
  }

}
