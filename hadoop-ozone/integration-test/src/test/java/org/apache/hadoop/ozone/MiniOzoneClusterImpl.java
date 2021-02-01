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
package org.apache.hadoop.ozone;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.recon.ConfigurationProvider;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.commons.io.FileUtils;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import org.hadoop.ozone.recon.codegen.ReconSqlDbConfig;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * MiniOzoneCluster creates a complete in-process Ozone cluster suitable for
 * running tests.  The cluster consists of a OzoneManager,
 * StorageContainerManager and multiple DataNodes.
 */
@InterfaceAudience.Private
public class MiniOzoneClusterImpl implements MiniOzoneCluster {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneClusterImpl.class);

  private final OzoneConfiguration conf;
  private StorageContainerManager scm;
  private OzoneManager ozoneManager;
  private final List<HddsDatanodeService> hddsDatanodes;
  private ReconServer reconServer;

  // Timeout for the cluster to be ready
  private int waitForClusterToBeReadyTimeout = 120000; // 2 min
  private CertificateClient caClient;

  /**
   * Creates a new MiniOzoneCluster.
   *
   * @throws IOException if there is an I/O error
   */
  protected MiniOzoneClusterImpl(OzoneConfiguration conf,
                                 OzoneManager ozoneManager,
                                 StorageContainerManager scm,
                                 List<HddsDatanodeService> hddsDatanodes) {
    this.conf = conf;
    this.ozoneManager = ozoneManager;
    this.scm = scm;
    this.hddsDatanodes = hddsDatanodes;
  }

  /**
   * Creates a new MiniOzoneCluster with Recon.
   *
   * @throws IOException if there is an I/O error
   */
  MiniOzoneClusterImpl(OzoneConfiguration conf,
      OzoneManager ozoneManager,
      StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes,
      ReconServer reconServer) {
    this.conf = conf;
    this.ozoneManager = ozoneManager;
    this.scm = scm;
    this.hddsDatanodes = hddsDatanodes;
    this.reconServer = reconServer;
  }

  /**
   * Creates a new MiniOzoneCluster without the OzoneManager. This is used by
   * {@link MiniOzoneHAClusterImpl} for starting multiple OzoneManagers.
   *
   * @param conf
   * @param scm
   * @param hddsDatanodes
   */
  MiniOzoneClusterImpl(OzoneConfiguration conf, StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes, ReconServer reconServer) {
    this.conf = conf;
    this.scm = scm;
    this.hddsDatanodes = hddsDatanodes;
    this.reconServer = reconServer;
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  public String getServiceId() {
    // Non-HA cluster doesn't have OM Service Id.
    return null;
  }

  /**
   * Waits for the Ozone cluster to be ready for processing requests.
   */
  @Override
  public void waitForClusterToBeReady()
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      final int healthy = scm.getNodeCount(HEALTHY);
      final boolean isNodeReady = healthy == hddsDatanodes.size();
      final boolean exitSafeMode = !scm.isInSafeMode();

      LOG.info("{}. Got {} of {} DN Heartbeats.",
          isNodeReady ? "Nodes are ready" : "Waiting for nodes to be ready",
          healthy, hddsDatanodes.size());
      LOG.info(exitSafeMode ? "Cluster exits safe mode" :
              "Waiting for cluster to exit safe mode",
          healthy, hddsDatanodes.size());

      return isNodeReady && exitSafeMode;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  /**
   * Waits for atleast one RATIS pipeline of given factor to be reported in open
   * state.
   */
  @Override
  public void waitForPipelineTobeReady(HddsProtos.ReplicationFactor factor,
                                int timeoutInMs) throws
          TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      int openPipelineCount = scm.getPipelineManager().
              getPipelines(HddsProtos.ReplicationType.RATIS,
              factor, Pipeline.PipelineState.OPEN).size();
      return openPipelineCount >= 1;
    }, 1000, timeoutInMs);
  }

  /**
   * Sets the timeout value after which
   * {@link MiniOzoneClusterImpl#waitForClusterToBeReady} times out.
   *
   * @param timeoutInMs timeout value in milliseconds
   */
  @Override
  public void setWaitForClusterToBeReadyTimeout(int timeoutInMs) {
    waitForClusterToBeReadyTimeout = timeoutInMs;
  }

  /**
   * Waits for SCM to be out of Safe Mode. Many tests can be run iff we are out
   * of Safe mode.
   *
   * @throws TimeoutException
   * @throws InterruptedException
   */
  @Override
  public void waitTobeOutOfSafeMode()
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      if (!scm.isInSafeMode()) {
        return true;
      }
      LOG.info("Waiting for cluster to be ready. No datanodes found");
      return false;
    }, 100, 1000 * 45);
  }

  @Override
  public StorageContainerManager getStorageContainerManager() {
    return this.scm;
  }

  @Override
  public OzoneManager getOzoneManager() {
    return this.ozoneManager;
  }

  @Override
  public List<HddsDatanodeService> getHddsDatanodes() {
    return hddsDatanodes;
  }

  @Override
  public ReconServer getReconServer() {
    return this.reconServer;
  }

  @Override
  public int getHddsDatanodeIndex(DatanodeDetails dn) throws IOException {
    for (HddsDatanodeService service : hddsDatanodes) {
      if (service.getDatanodeDetails().equals(dn)) {
        return hddsDatanodes.indexOf(service);
      }
    }
    throw new IOException(
        "Not able to find datanode with datanode Id " + dn.getUuid());
  }

  @Override
  public OzoneClient getClient() throws IOException {
    return OzoneClientFactory.getRpcClient(conf);
  }

  @Override
  public OzoneClient getRpcClient() throws IOException {
    return OzoneClientFactory.getRpcClient(conf);
  }

  /**
   * Returns an RPC proxy connected to this cluster's StorageContainerManager
   * for accessing container location information.  Callers take ownership of
   * the proxy and must close it when done.
   *
   * @return RPC proxy for accessing container location information
   * @throws IOException if there is an I/O error
   */
  @Override
  public StorageContainerLocationProtocolClientSideTranslatorPB
      getStorageContainerLocationClient() throws IOException {
    long version = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    InetSocketAddress address = scm.getClientRpcAddress();
    LOG.info(
        "Creating StorageContainerLocationProtocol RPC client with address {}",
        address);
    return new StorageContainerLocationProtocolClientSideTranslatorPB(
        RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
            address, UserGroupInformation.getCurrentUser(), conf,
            NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf)));
  }

  @Override
  public void restartStorageContainerManager(boolean waitForDatanode)
      throws TimeoutException, InterruptedException, IOException,
      AuthenticationException {
    scm.stop();
    scm.join();
    scm = StorageContainerManager.createSCM(conf);
    scm.start();
    if (waitForDatanode) {
      waitForClusterToBeReady();
    }
  }

  @Override
  public void restartOzoneManager() throws IOException {
    ozoneManager.stop();
    ozoneManager.restart();
  }

  @Override
  public void restartReconServer() {
    stopRecon(reconServer);
    startRecon();
  }

  private void waitForHddsDatanodesStop() throws TimeoutException,
      InterruptedException {
    GenericTestUtils.waitFor(() -> {
      final int healthy = scm.getNodeCount(HEALTHY);
      boolean isReady = healthy == hddsDatanodes.size();
      if (!isReady) {
        LOG.info("Waiting on {} datanodes out of {} to be marked unhealthy.",
            healthy, hddsDatanodes.size());
      }
      return isReady;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  @Override
  public void restartHddsDatanode(int i, boolean waitForDatanode)
      throws InterruptedException, TimeoutException {
    HddsDatanodeService datanodeService = hddsDatanodes.get(i);
    stopDatanode(datanodeService);
    // ensure same ports are used across restarts.
    OzoneConfiguration config = datanodeService.getConf();
    int currentPort = datanodeService.getDatanodeDetails()
        .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    config.setInt(DFS_CONTAINER_IPC_PORT, currentPort);
    config.setBoolean(DFS_CONTAINER_IPC_RANDOM_PORT, false);
    int ratisPort = datanodeService.getDatanodeDetails()
        .getPort(DatanodeDetails.Port.Name.RATIS).getValue();
    config.setInt(DFS_CONTAINER_RATIS_IPC_PORT, ratisPort);
    config.setBoolean(DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, false);
    hddsDatanodes.remove(i);
    if (waitForDatanode) {
      // wait for node to be removed from SCM healthy node list.
      waitForHddsDatanodesStop();
    }
    String[] args = new String[] {};
    HddsDatanodeService service =
        HddsDatanodeService.createHddsDatanodeService(args);
    hddsDatanodes.add(i, service);
    service.start(config);
    if (waitForDatanode) {
      // wait for the node to be identified as a healthy node again.
      waitForClusterToBeReady();
    }
  }

  @Override
  public void restartHddsDatanode(DatanodeDetails dn, boolean waitForDatanode)
      throws InterruptedException, TimeoutException, IOException {
    restartHddsDatanode(getHddsDatanodeIndex(dn), waitForDatanode);
  }

  @Override
  public void shutdownHddsDatanode(int i) {
    stopDatanode(hddsDatanodes.get(i));
  }

  @Override
  public void shutdownHddsDatanode(DatanodeDetails dn) throws IOException {
    shutdownHddsDatanode(getHddsDatanodeIndex(dn));
  }

  @Override
  public void shutdown() {
    try {
      LOG.info("Shutting down the Mini Ozone Cluster");
      File baseDir = new File(GenericTestUtils.getTempPath(
          MiniOzoneClusterImpl.class.getSimpleName() + "-" +
              scm.getClientProtocolServer().getScmInfo().getClusterId()));
      stop();
      FileUtils.deleteDirectory(baseDir);
      ContainerCache.getInstance(conf).shutdownCache();
      DefaultMetricsSystem.shutdown();
    } catch (IOException e) {
      LOG.error("Exception while shutting down the cluster.", e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping the Mini Ozone Cluster");
    stopOM(ozoneManager);
    stopDatanodes(hddsDatanodes);
    stopSCM(scm);
    stopRecon(reconServer);
  }

  /**
   * Start Scm.
   */
  @Override
  public void startScm() throws IOException {
    scm.start();
  }

  /**
   * Start DataNodes.
   */
  @Override
  public void startHddsDatanodes() {
    hddsDatanodes.forEach((datanode) -> {
      datanode.setCertificateClient(getCAClient());
      datanode.start();
    });
  }

  @Override
  public void shutdownHddsDatanodes() {
    hddsDatanodes.forEach((datanode) -> {
      try {
        shutdownHddsDatanode(datanode.getDatanodeDetails());
      } catch (IOException e) {
        LOG.error("Exception while trying to shutdown datanodes:", e);
      }
    });
  }

  @Override
  public void startRecon() {
    reconServer = new ReconServer();
    reconServer.execute(new String[]{});
  }

  @Override
  public void stopRecon() {
    stopRecon(reconServer);
  }

  private CertificateClient getCAClient() {
    return this.caClient;
  }

  private void setCAClient(CertificateClient client) {
    this.caClient = client;
  }

  private static void stopDatanodes(
      Collection<HddsDatanodeService> hddsDatanodes) {
    if (!hddsDatanodes.isEmpty()) {
      LOG.info("Stopping the HddsDatanodes");
      hddsDatanodes.parallelStream()
          .forEach(MiniOzoneClusterImpl::stopDatanode);
    }
  }

  private static void stopDatanode(HddsDatanodeService dn) {
    if (dn != null) {
      dn.stop();
      dn.join();
    }
  }

  private static void stopSCM(StorageContainerManager scm) {
    if (scm != null) {
      LOG.info("Stopping the StorageContainerManager");
      scm.stop();
      scm.join();
    }
  }

  private static void stopOM(OzoneManager om) {
    if (om != null) {
      LOG.info("Stopping the OzoneManager");
      om.stop();
      om.join();
    }
  }

  private static void stopRecon(ReconServer reconServer) {
    try {
      if (reconServer != null) {
        LOG.info("Stopping Recon");
        reconServer.stop();
        reconServer.join();
      }
    } catch (Exception e) {
      LOG.error("Exception while shutting down Recon.", e);
    }
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder extends MiniOzoneCluster.Builder {

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
    }

    @Override
    public MiniOzoneCluster build() throws IOException {
      DefaultMetricsSystem.setMiniClusterMode(true);
      initializeConfiguration();
      StorageContainerManager scm = null;
      OzoneManager om = null;
      ReconServer reconServer = null;
      List<HddsDatanodeService> hddsDatanodes = Collections.emptyList();
      try {
        scm = createSCM();
        scm.start();
        om = createOM();
        if (certClient != null) {
          om.setCertClient(certClient);
        }
        om.start();

        if (includeRecon) {
          configureRecon();
          reconServer = new ReconServer();
          reconServer.execute(new String[] {});
        }

        hddsDatanodes = createHddsDatanodes(scm, reconServer);

        MiniOzoneClusterImpl cluster = new MiniOzoneClusterImpl(conf, om, scm,
            hddsDatanodes, reconServer);

        cluster.setCAClient(certClient);
        if (startDataNodes) {
          cluster.startHddsDatanodes();
        }
        return cluster;
      } catch (Exception ex) {
        stopOM(om);
        if (includeRecon) {
          stopRecon(reconServer);
        }
        if (startDataNodes) {
          stopDatanodes(hddsDatanodes);
        }
        stopSCM(scm);
        removeConfiguration();

        if (ex instanceof IOException) {
          throw (IOException) ex;
        }
        if (ex instanceof RuntimeException) {
          throw (RuntimeException) ex;
        }
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }
    }

    /**
     * Initializes the configuration required for starting MiniOzoneCluster.
     *
     * @throws IOException
     */
    protected void initializeConfiguration() throws IOException {
      Path metaDir = Paths.get(path, "ozone-meta");
      Files.createDirectories(metaDir);
      conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
      if (!chunkSize.isPresent()) {
        //set it to 1MB by default in tests
        chunkSize = Optional.of(1);
      }
      if (!streamBufferSize.isPresent()) {
        streamBufferSize = OptionalInt.of(chunkSize.get());
      }
      if (!streamBufferFlushSize.isPresent()) {
        streamBufferFlushSize = Optional.of((long) chunkSize.get());
      }
      if (!streamBufferMaxSize.isPresent()) {
        streamBufferMaxSize = Optional.of(2 * streamBufferFlushSize.get());
      }
      if (!blockSize.isPresent()) {
        blockSize = Optional.of(2 * streamBufferMaxSize.get());
      }

      if (!streamBufferSizeUnit.isPresent()) {
        streamBufferSizeUnit = Optional.of(StorageUnit.MB);
      }

      OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamBufferSize(
          (int) Math.round(
              streamBufferSizeUnit.get().toBytes(streamBufferSize.getAsInt())));
      clientConfig.setStreamBufferMaxSize(Math.round(
          streamBufferSizeUnit.get().toBytes(streamBufferMaxSize.get())));
      clientConfig.setStreamBufferFlushSize(Math.round(
          streamBufferSizeUnit.get().toBytes(streamBufferFlushSize.get())));
      conf.setFromObject(clientConfig);

      conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
          chunkSize.get(), streamBufferSizeUnit.get());

      conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, blockSize.get(),
          streamBufferSizeUnit.get());
      // MiniOzoneCluster should have global pipeline upper limit.
      conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT,
          pipelineNumLimit >= DEFAULT_PIPELIME_LIMIT ?
              pipelineNumLimit : DEFAULT_PIPELIME_LIMIT);
      conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          DEFAULT_RATIS_RPC_TIMEOUT_SEC, TimeUnit.SECONDS);
      configureTrace();
    }

    void removeConfiguration() {
      FileUtils.deleteQuietly(new File(path));
    }

    /**
     * Creates a new StorageContainerManager instance.
     *
     * @return {@link StorageContainerManager}
     * @throws IOException
     */
    protected StorageContainerManager createSCM()
        throws IOException, AuthenticationException {
      configureSCM();
      SCMStorageConfig scmStore = new SCMStorageConfig(conf);
      initializeScmStorage(scmStore);
      StorageContainerManager scm = StorageContainerManager.createSCM(conf);
      HealthyPipelineSafeModeRule rule =
          scm.getScmSafeModeManager().getHealthyPipelineSafeModeRule();
      if (rule != null) {
        // Set threshold to wait for safe mode exit - this is needed since a
        // pipeline is marked open only after leader election.
        rule.setHealthyPipelineThresholdCount(numOfDatanodes / 3);
      }
      return scm;
    }

    private void initializeScmStorage(SCMStorageConfig scmStore)
        throws IOException {
      if (scmStore.getState() == StorageState.INITIALIZED) {
        return;
      }
      scmStore.setClusterId(clusterId);
      if (!scmId.isPresent()) {
        scmId = Optional.of(UUID.randomUUID().toString());
      }
      scmStore.setScmId(scmId.get());
      scmStore.initialize();
    }

    void initializeOmStorage(OMStorage omStorage) throws IOException {
      if (omStorage.getState() == StorageState.INITIALIZED) {
        return;
      }
      omStorage.setClusterId(clusterId);
      omStorage.setScmId(scmId.get());
      omStorage.setOmId(omId.orElse(UUID.randomUUID().toString()));
      // Initialize ozone certificate client if security is enabled.
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        OzoneManager.initializeSecurity(conf, omStorage);
      }
      omStorage.initialize();
    }

    /**
     * Creates a new OzoneManager instance.
     *
     * @return {@link OzoneManager}
     * @throws IOException
     */
    protected OzoneManager createOM()
        throws IOException, AuthenticationException {
      configureOM();
      OMStorage omStore = new OMStorage(conf);
      initializeOmStorage(omStore);
      return OzoneManager.createOm(conf);
    }

    /**
     * Creates HddsDatanodeService(s) instance.
     *
     * @return List of HddsDatanodeService
     * @throws IOException
     */
    protected List<HddsDatanodeService> createHddsDatanodes(
        StorageContainerManager scm, ReconServer reconServer)
        throws IOException {
      configureHddsDatanodes();
      String scmAddress = scm.getDatanodeRpcAddress().getHostString() +
          ":" + scm.getDatanodeRpcAddress().getPort();
      String[] args = new String[] {};
      conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, scmAddress);
      List<HddsDatanodeService> hddsDatanodes = new ArrayList<>();
      for (int i = 0; i < numOfDatanodes; i++) {
        OzoneConfiguration dnConf = new OzoneConfiguration(conf);
        String datanodeBaseDir = path + "/datanode-" + Integer.toString(i);
        Path metaDir = Paths.get(datanodeBaseDir, "meta");
        List<String> dataDirs = new ArrayList<>();
        for (int j = 0; j < numDataVolumes; j++) {
          Path dir = Paths.get(datanodeBaseDir, "data-" + j, "containers");
          Files.createDirectories(dir);
          dataDirs.add(dir.toString());
        }
        String listOfDirs = String.join(",", dataDirs);
        Path ratisDir = Paths.get(datanodeBaseDir, "data", "ratis");
        Path workDir = Paths.get(datanodeBaseDir, "data", "replication",
            "work");
        Files.createDirectories(metaDir);
        Files.createDirectories(ratisDir);
        Files.createDirectories(workDir);
        dnConf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
        dnConf.set(DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY, listOfDirs);
        dnConf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
            ratisDir.toString());
        dnConf.set(OzoneConfigKeys.OZONE_CONTAINER_COPY_WORKDIR,
            workDir.toString());
        if (reconServer != null) {
          OzoneStorageContainerManager reconScm =
              reconServer.getReconStorageContainerManager();
          dnConf.set(OZONE_RECON_ADDRESS_KEY,
              reconScm.getDatanodeRpcAddress().getHostString() + ":" +
                  reconScm.getDatanodeRpcAddress().getPort());
        }

        HddsDatanodeService datanode
            = HddsDatanodeService.createHddsDatanodeService(args);
        datanode.setConfiguration(dnConf);
        hddsDatanodes.add(datanode);
      }
      return hddsDatanodes;
    }

    private void configureSCM() {
      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.setInt(ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY, numOfScmHandlers);
      conf.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
          "3s");
      configureSCMheartbeat();
    }

    private void configureSCMheartbeat() {
      if (hbInterval.isPresent()) {
        conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
            hbInterval.get(), TimeUnit.MILLISECONDS);
      } else {
        conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
            DEFAULT_HB_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
      }

      if (hbProcessorInterval.isPresent()) {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            hbProcessorInterval.get(),
            TimeUnit.MILLISECONDS);
      } else {
        conf.setTimeDuration(
            ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            DEFAULT_HB_PROCESSOR_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
      }
    }

    private void configureOM() {
      conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numOfOmHandlers);
    }

    private void configureHddsDatanodes() {
      conf.set(ScmConfigKeys.HDDS_REST_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      conf.set(HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT,
          randomContainerPort);
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
          randomContainerPort);

      conf.setFromObject(new ReplicationConfig().setPort(0));
    }

    private void configureTrace() {
      if (enableTrace.isPresent()) {
        conf.setBoolean(OzoneConfigKeys.OZONE_TRACE_ENABLED_KEY,
            enableTrace.get());
        GenericTestUtils.setRootLogLevel(Level.TRACE);
      }
      GenericTestUtils.setRootLogLevel(Level.INFO);
    }

    protected void configureRecon() throws IOException {
      ConfigurationProvider.resetConfiguration();

      TemporaryFolder tempFolder = new TemporaryFolder();
      tempFolder.create();
      File tempNewFolder = tempFolder.newFolder();
      conf.set(OZONE_RECON_DB_DIR,
          tempNewFolder.getAbsolutePath());
      conf.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR, tempNewFolder
          .getAbsolutePath());
      conf.set(OZONE_RECON_SCM_DB_DIR,
          tempNewFolder.getAbsolutePath());

      ReconSqlDbConfig dbConfig = conf.getObject(ReconSqlDbConfig.class);
      dbConfig.setJdbcUrl("jdbc:derby:" + tempNewFolder.getAbsolutePath()
          + "/ozone_recon_derby.db");
      conf.setFromObject(dbConfig);

      conf.set(OZONE_RECON_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      conf.set(OZONE_RECON_DATANODE_ADDRESS_KEY, "0.0.0.0:0");

      ConfigurationProvider.setConfiguration(conf);
    }
  }
}
