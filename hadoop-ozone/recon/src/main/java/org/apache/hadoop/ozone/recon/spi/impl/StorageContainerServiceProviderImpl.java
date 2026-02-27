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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_SNAPSHOT_DB;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.ha.InterSCMGrpcClient;
import org.apache.hadoop.hdds.scm.ha.SCMSnapshotDownloader;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.ozone.recon.security.ReconCertificateClient;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.ratis.proto.RaftProtos;
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
  private File scmSnapshotDBParentDir;
  private ReconUtils reconUtils;
  private ReconStorageConfig reconStorage;
  private ReconContext reconContext;

  @Inject
  public StorageContainerServiceProviderImpl(
      StorageContainerLocationProtocol scmClient,
      ReconUtils reconUtils,
      OzoneConfiguration configuration,
      ReconStorageConfig reconStorage,
      ReconContext reconContext) {

    scmSnapshotDBParentDir = ReconUtils.getReconScmDbDir(configuration);

    this.reconUtils = reconUtils;
    this.scmClient = scmClient;
    this.configuration = configuration;
    this.reconStorage = reconStorage;
    this.reconContext = reconContext;
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
        "", ClientVersion.CURRENT_VERSION);
  }

  @Override
  public long getContainerCount() throws IOException {
    return scmClient.getContainerCount();
  }

  @Override
  public long getContainerCount(HddsProtos.LifeCycleState state)
      throws IOException {
    return scmClient.getContainerCount(state);
  }

  @Override
  public DBCheckpoint getSCMDBSnapshot() {
    String snapshotFileName = RECON_SCM_SNAPSHOT_DB + "_" +
        System.currentTimeMillis();
    File targetFile = new File(scmSnapshotDBParentDir, snapshotFileName +
        ".tar");
    try {
      try {
        List<String> ratisRoles = scmClient.getScmInfo().getPeerRoles();
        for (String ratisRole : ratisRoles) {
          String[] role = ratisRole.split(":");
          if (role[2].equals(RaftProtos.RaftPeerRole.LEADER.toString())) {
            String hostAddress = role[4].trim();
            int grpcPort = configuration.getInt(
                ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY,
                ScmConfigKeys.OZONE_SCM_GRPC_PORT_DEFAULT);

            SecurityConfig secConf = new SecurityConfig(configuration);
            SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
                getScmSecurityClientWithMaxRetry(
                    configuration, getCurrentUser());
            try (ReconCertificateClient certClient =
                     new ReconCertificateClient(
                         secConf, scmSecurityClient, reconStorage, null, null);
                 SCMSnapshotDownloader downloadClient = new InterSCMGrpcClient(
                     hostAddress, grpcPort, configuration, certClient)) {
              downloadClient.download(targetFile.toPath()).get();
            } catch (ExecutionException | InterruptedException e) {
              LOG.error("Rocks DB checkpoint downloading failed: {}", e);
              throw new IOException(e);
            }
            LOG.info("Downloaded SCM Snapshot from Leader SCM");
            break;
          }
        }
      } catch (Throwable throwable) {
        LOG.error("Unexpected runtime error while downloading SCM Rocks DB snapshot/checkpoint : {}", throwable);
        throw throwable;
      }
      return getRocksDBCheckpoint(snapshotFileName, targetFile);
    } catch (Throwable e) {
      reconContext.updateHealthStatus(new AtomicBoolean(false));
      reconContext.getErrors().add(ReconContext.ErrorCode.GET_SCM_DB_SNAPSHOT_FAILED);
      LOG.error("Unable to obtain SCM DB Snapshot: {} ", e);
    }
    return null;
  }

  @NotNull
  private RocksDBCheckpoint getRocksDBCheckpoint(String snapshotFileName, File targetFile) throws IOException {
    Path untarredDbDir = Paths.get(scmSnapshotDBParentDir.getAbsolutePath(),
        snapshotFileName);
    reconUtils.untarCheckpointFile(targetFile, untarredDbDir);
    FileUtils.deleteQuietly(targetFile);
    return new RocksDBCheckpoint(untarredDbDir);
  }

  @Override
  public List<ContainerID> getListOfContainerIDs(
      ContainerID startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException {
    return scmClient.getListOfContainerIDs(startContainerID, count, state);
  }

  @Override
  public List<ContainerInfo> getListOfContainers(
      long startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException {
    return scmClient.getListOfContainers(startContainerID, count, state);
  }

}
