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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONNECTION_TIMEOUT_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for StorageContainerServiceProvider that talks with actual
 * cluster SCM.
 */
public class StorageContainerServiceProviderImpl
    implements StorageContainerServiceProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerServiceProviderImpl.class);
  private StorageContainerLocationProtocol scmClient;
  private final OzoneConfiguration configuration;
  private String scmDBSnapshotUrl;
  private File scmSnapshotDBParentDir;
  private URLConnectionFactory connectionFactory;
  private ReconUtils reconUtils;

  @Inject
  public StorageContainerServiceProviderImpl(
      StorageContainerLocationProtocol scmClient,
      ReconUtils reconUtils,
      OzoneConfiguration configuration) {

    int connectionTimeout = (int) configuration.getTimeDuration(
        OZONE_RECON_SCM_CONNECTION_TIMEOUT,
        OZONE_RECON_SCM_CONNECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    int connectionRequestTimeout = (int) configuration.getTimeDuration(
        OZONE_RECON_SCM_CONNECTION_REQUEST_TIMEOUT,
        OZONE_RECON_SCM_CONNECTION_REQUEST_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(connectionTimeout,
                connectionRequestTimeout, configuration);

    String scmHttpAddress = configuration.get(ScmConfigKeys
        .OZONE_SCM_HTTP_ADDRESS_KEY);

    String scmHttpsAddress = configuration.get(ScmConfigKeys
        .OZONE_SCM_HTTPS_ADDRESS_KEY);

    HttpConfig.Policy policy = HttpConfig.getHttpPolicy(configuration);

    scmSnapshotDBParentDir = ReconUtils.getReconScmDbDir(configuration);

    scmDBSnapshotUrl = "http://" + scmHttpAddress +
            OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;

    if (policy.isHttpsEnabled()) {
      scmDBSnapshotUrl = "https://" + scmHttpsAddress +
              OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
    }

    this.reconUtils = reconUtils;
    this.scmClient = scmClient;
    this.configuration = configuration;
  }

  @Override
  public List<Pipeline> getPipelines() throws IOException {
    return scmClient.listPipelines();
  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    return scmClient.getPipeline(pipelineID);
  }

  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException {
    return scmClient.getContainerWithPipeline(containerId);
  }

  @Override
  public List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
      List<Long> containerIDs) {
    return scmClient.getExistContainerWithPipelinesInBatch(containerIDs);
  }

  @Override
  public List<HddsProtos.Node> getNodes() throws IOException {
    return scmClient.queryNode(null, null, HddsProtos.QueryScope.CLUSTER,
        "", CURRENT_VERSION);
  }

  @Override
  public long getSCMContainersCount() throws IOException {
    return scmClient.getSCMContainersCount();
  }

  public String getScmDBSnapshotUrl() {
    return scmDBSnapshotUrl;
  }

  private boolean isOmSpnegoEnabled() {
    return configuration.get(HDDS_SCM_HTTP_AUTH_TYPE, "simple")
        .equals("kerberos");
  }

  public DBCheckpoint getSCMDBSnapshot() {
    String snapshotFileName = RECON_SCM_SNAPSHOT_DB + "_" +
        System.currentTimeMillis();
    File targetFile = new File(scmSnapshotDBParentDir, snapshotFileName +
            ".tar.gz");

    try {
      SecurityUtil.doAsLoginUser(() -> {
        try (InputStream inputStream = reconUtils.makeHttpCall(
            connectionFactory, getScmDBSnapshotUrl(),
            isOmSpnegoEnabled()).getInputStream()) {
          FileUtils.copyInputStreamToFile(inputStream, targetFile);
        }
        return null;
      });

      Path untarredDbDir = Paths.get(scmSnapshotDBParentDir.getAbsolutePath(),
          snapshotFileName);
      reconUtils.untarCheckpointFile(targetFile, untarredDbDir);
      FileUtils.deleteQuietly(targetFile);
      return new RocksDBCheckpoint(untarredDbDir);
    } catch (IOException e) {
      LOG.error("Unable to obtain SCM DB Snapshot. ", e);
    }
    return null;
  }
}
