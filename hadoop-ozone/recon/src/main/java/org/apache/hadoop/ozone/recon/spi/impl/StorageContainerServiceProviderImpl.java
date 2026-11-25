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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.ratisSnapshotComplete;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_POLICY_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_READ_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_READ_TIMEOUT_DEFAULT;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.ha.InterSCMGrpcClient;
import org.apache.hadoop.hdds.scm.ha.SCMSnapshotDownloader;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationInternalInterface;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.ozone.recon.security.ReconCertificateClient;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.security.SecurityUtil;
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
  private StorageContainerLocationInternalInterface scmClient;
  private final OzoneConfiguration configuration;
  private File scmSnapshotDBParentDir;
  private ReconUtils reconUtils;
  private ReconStorageConfig reconStorage;
  private ReconContext reconContext;

  @Inject
  public StorageContainerServiceProviderImpl(
      StorageContainerLocationInternalInterface scmClient,
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
  public ContainerInfo getContainerInfo(long containerId) throws IOException {
    try {
      return scmClient.getContainer(containerId);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException(ContainerNotFoundException.class);
    }
  }

  @Override
  public List<ContainerInfo> getExistingContainersInBatch(List<ContainerID> containerIDs) throws IOException {
    return scmClient.getExistingContainersInBatch(containerIDs.stream().map(ContainerID::getId)
        .collect(Collectors.toList()));
  }

  @Override
  public List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
      List<Long> containerIDs) {
    return scmClient.getExistContainerWithPipelinesInBatch(containerIDs);
  }

  @Override
  public List<HddsProtos.Node> getNodes() throws IOException {
    return scmClient.queryNode(null, null, HddsProtos.QueryScope.CLUSTER,
        "", ClientVersion.CURRENT);
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
          String[] role = HddsUtils.parseRatisRoleString(ratisRole);
          if (role[2].equals(RaftProtos.RaftPeerRole.LEADER.toString())) {
            SecurityConfig secConf = new SecurityConfig(configuration);
            if (secConf.isSecurityEnabled() && secConf.isGrpcTlsEnabled()) {
              downloadSCMDBCheckpoint(getSCMDBCheckpointUrl(role[0], Integer.parseInt(role[1])), targetFile);
            } else {
              downloadSCMDBCheckpointOverGrpc(role[4].trim(), targetFile, secConf);
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

  @Nonnull
  private RocksDBCheckpoint getRocksDBCheckpoint(String snapshotFileName, File targetFile) throws IOException {
    Path untarredDbDir = Paths.get(scmSnapshotDBParentDir.getAbsolutePath(),
        snapshotFileName);
    reconUtils.untarCheckpointFile(targetFile, untarredDbDir);
    FileUtils.deleteQuietly(targetFile);
    // SCM writes this flag last, so a checkpoint without it was cut short.
    if (!ratisSnapshotComplete(untarredDbDir)) {
      FileUtils.deleteQuietly(untarredDbDir.toFile());
      throw new IOException("Incomplete SCM DB checkpoint: " + OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME + " not found");
    }
    return new RocksDBCheckpoint(untarredDbDir);
  }

  private void downloadSCMDBCheckpointOverGrpc(String hostAddress, File targetFile, SecurityConfig secConf)
      throws IOException {
    int grpcPort =
        configuration.getInt(ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, ScmConfigKeys.OZONE_SCM_GRPC_PORT_DEFAULT);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        getScmSecurityClientWithMaxRetry(configuration, getCurrentUser());
    try (ReconCertificateClient certClient =
             new ReconCertificateClient(secConf, scmSecurityClient, reconStorage, null, null);
         SCMSnapshotDownloader downloadClient =
             new InterSCMGrpcClient(hostAddress, grpcPort, configuration, certClient)) {
      downloadClient.download(targetFile.toPath()).get();
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Rocks DB checkpoint downloading failed: {}", e);
      throw new IOException(e);
    }
  }

  /**
   * Downloads the SCM DB checkpoint over HTTP(S), authenticated with SPNEGO when SCM requires it.
   * Used when gRPC TLS is enabled, because the inter-SCM gRPC server then accepts SCM certificates only.
   */
  private void downloadSCMDBCheckpoint(String url, File targetFile) throws IOException {
    LOG.info("Downloading SCM DB checkpoint from {}", url);
    int connectTimeout = (int) configuration.getTimeDuration(OZONE_RECON_SCM_SNAPSHOT_CONNECTION_TIMEOUT,
        OZONE_RECON_SCM_SNAPSHOT_CONNECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    int readTimeout = (int) configuration.getTimeDuration(OZONE_RECON_SCM_SNAPSHOT_READ_TIMEOUT,
        OZONE_RECON_SCM_SNAPSHOT_READ_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(connectTimeout, readTimeout, configuration);
    boolean spnegoEnabled = "kerberos".equals(configuration.get(HDDS_SCM_HTTP_AUTH_TYPE, "simple"));
    AtomicBoolean cancelled = new AtomicBoolean();
    // Download in another thread, so that the sync can return as soon as it is cancelled (interrupted).
    ExecutorService executor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("ReconSCMDBDownload-%d").setDaemon(true).build());
    Future<?> download = executor.submit(() -> SecurityUtil.doAsLoginUser(() -> {
      HttpURLConnection connection = null;
      try {
        connection = reconUtils.makeHttpCall(connectionFactory, url, spnegoEnabled);
        try (InputStream in = connection.getInputStream();
             OutputStream out = Files.newOutputStream(targetFile.toPath())) {
          byte[] buffer = new byte[IOUtils.DEFAULT_BUFFER_SIZE];
          for (int n; (n = in.read(buffer)) != -1;) {
            if (cancelled.get()) {
              throw new InterruptedIOException("SCM DB checkpoint download cancelled");
            }
            out.write(buffer, 0, n);
          }
        }
      } catch (Exception e) {
        FileUtils.deleteQuietly(targetFile);
        throw e;
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
        connectionFactory.destroy();
        if (cancelled.get()) {
          FileUtils.deleteQuietly(targetFile);
        }
      }
      return null;
    }));
    executor.shutdown();
    try {
      download.get();
    } catch (InterruptedException e) {
      // A blocked HttpURLConnection read cannot be aborted from another thread: closing its stream or disconnecting
      // waits for the reader. So do not wait for it. The download stops at its next read or its read timeout.
      cancelled.set(true);
      FileUtils.deleteQuietly(targetFile);
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("SCM DB checkpoint download cancelled");
    } catch (ExecutionException e) {
      throw e.getCause() instanceof IOException ? (IOException) e.getCause() : new IOException(e.getCause());
    }
  }

  /**
   * Returns the DB checkpoint URL of the SCM with Ratis address {@code host:ratisPort}. The HTTP(S) port is taken from
   * the address configured for that SCM node, else from the common SCM HTTP(S) address.
   */
  private String getSCMDBCheckpointUrl(String host, int ratisPort) {
    boolean httpsEnabled = HttpConfig.getHttpPolicy(configuration).isHttpsEnabled();
    if (!httpsEnabled) {
      LOG.warn("Downloading the SCM DB unencrypted over HTTP although {} is true. Set {} to HTTPS_ONLY or "
          + "HTTP_AND_HTTPS to encrypt it.", HDDS_GRPC_TLS_ENABLED, OZONE_HTTP_POLICY_KEY);
    }
    String addressKey = httpsEnabled ? OZONE_SCM_HTTPS_ADDRESS_KEY : OZONE_SCM_HTTP_ADDRESS_KEY;
    List<String> addressKeys = new ArrayList<>();
    String serviceId = HddsUtils.getScmServiceId(configuration);
    if (serviceId != null) {
      for (String nodeId : HddsUtils.getSCMNodeIds(configuration, serviceId)) {
        // Match the node's Ratis address the way SCMHANodeDetails builds it, since SCMs may share a host.
        String scmAddress =
            configuration.getTrimmed(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY, serviceId, nodeId));
        int nodeRatisPort = HddsUtils.getHostPort(scmAddress).orElse(configuration.getInt(
            ConfUtils.addKeySuffixes(OZONE_SCM_RATIS_PORT_KEY, serviceId, nodeId),
            configuration.getInt(OZONE_SCM_RATIS_PORT_KEY, OZONE_SCM_RATIS_PORT_DEFAULT)));
        if (HddsUtils.getHostName(scmAddress).filter(host::equals).isPresent() && nodeRatisPort == ratisPort) {
          addressKeys.add(ConfUtils.addKeySuffixes(addressKey, serviceId, nodeId));
        }
      }
    }
    addressKeys.add(addressKey);
    int port = HddsUtils.getPortNumberFromConfigKeys(configuration, addressKeys.toArray(new String[0]))
        .orElse(httpsEnabled ? OZONE_SCM_HTTPS_BIND_PORT_DEFAULT : OZONE_SCM_HTTP_BIND_PORT_DEFAULT);
    return (httpsEnabled ? "https://" : "http://") + HddsUtils.getHostPortString(host, port)
        + OZONE_DB_CHECKPOINT_HTTP_ENDPOINT + "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
  }

  @Override
  public List<ContainerID> getListOfContainerIDs(
      ContainerID startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException {
    return scmClient.getListOfContainerIDs(startContainerID, count, state);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to {@code SCM.listContainer(startId, count, state)} which
   * already has server-side pagination support. This reuses the existing RPC
   * without requiring a new protobuf message definition.
   */
  @Override
  public List<ContainerInfo> getListOfContainerInfos(
      ContainerID startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException {
    return scmClient.listContainer(
        startContainerID.getId(), count, state).getContainerInfoList();
  }

}
