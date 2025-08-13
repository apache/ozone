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

package org.apache.hadoop.ozone;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.server.http.BaseHttpServer.SERVER_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RATIS_SNAPSHOT_DIR;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.getFreePort;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.safemode.SafeModeRuleFactory;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectMetrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MiniOzoneCluster creates a complete in-process Ozone cluster suitable for
 * running tests.  The cluster consists of a OzoneManager,
 * StorageContainerManager and multiple DataNodes.
 */
@InterfaceAudience.Private
public class MiniOzoneClusterImpl implements MiniOzoneCluster {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneClusterImpl.class);

  private static final String[] NO_ARGS = new String[0];

  private static final String SCM_SUBDIR_NAME = "scm";
  private static final String OM_SUBDIR_NAME = "om";
  private static final String OZONE_METADATA_SUBDIR_NAME = "ozone-metadata";
  private static final String RATIS_SUBDIR_NAME = "ratis";
  private static final String DATA_SUBDIR_NAME = "data";

  static {
    CodecBuffer.enableLeakDetection();
  }

  private OzoneConfiguration conf;
  private final SCMConfigurator scmConfigurator;
  private StorageContainerManager scm;
  private OzoneManager ozoneManager;
  private final List<HddsDatanodeService> hddsDatanodes;
  private final List<Service> services;

  // Timeout for the cluster to be ready
  private int waitForClusterToBeReadyTimeout = 120000; // 2 min
  private CertificateClient caClient;
  private final Set<AutoCloseable> clients = ConcurrentHashMap.newKeySet();
  private SecretKeyClient secretKeyClient;

  private MiniOzoneClusterImpl(OzoneConfiguration conf,
      SCMConfigurator scmConfigurator,
      OzoneManager ozoneManager,
      StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes,
      List<Service> services) {
    this.conf = conf;
    this.ozoneManager = ozoneManager;
    this.scm = scm;
    this.hddsDatanodes = hddsDatanodes;
    this.scmConfigurator = scmConfigurator;
    this.services = services;
  }

  /**
   * Creates a new MiniOzoneCluster without the OzoneManager and
   * StorageContainerManager. This is used by
   * {@link MiniOzoneHAClusterImpl} for starting multiple
   * OzoneManagers and StorageContainerManagers.
   */
  MiniOzoneClusterImpl(OzoneConfiguration conf, SCMConfigurator scmConfigurator,
      List<HddsDatanodeService> hddsDatanodes, List<Service> services) {
    this.scmConfigurator = scmConfigurator;
    this.conf = conf;
    this.hddsDatanodes = hddsDatanodes;
    this.services = services;
  }

  public SCMConfigurator getSCMConfigurator() {
    return scmConfigurator;
  }

  @Override
  public OzoneConfiguration getConf() {
    return conf;
  }

  protected void setConf(OzoneConfiguration newConf) {
    this.conf = newConf;
  }

  public void waitForSCMToBeReady() throws TimeoutException,
      InterruptedException {
    GenericTestUtils.waitFor(scm::checkLeader,
        1000, waitForClusterToBeReadyTimeout);
  }

  public StorageContainerManager getActiveSCM() {
    return scm;
  }

  @Override
  public void waitForClusterToBeReady()
      throws TimeoutException, InterruptedException {
    waitForSCMToBeReady();
    GenericTestUtils.waitFor(() -> {
      StorageContainerManager activeScm = getActiveSCM();
      final int healthy = activeScm.getNodeCount(HEALTHY);
      final boolean isNodeReady = healthy == hddsDatanodes.size();
      final boolean exitSafeMode = !activeScm.isInSafeMode();
      final boolean checkScmLeader = activeScm.checkLeader();

      LOG.info("{}. Got {} of {} DN Heartbeats.",
          isNodeReady ? "Nodes are ready" : "Waiting for nodes to be ready",
          healthy, hddsDatanodes.size());
      LOG.info(exitSafeMode ? "Cluster exits safe mode" :
              "Waiting for cluster to exit safe mode");
      LOG.info(checkScmLeader ? "SCM became leader" :
          "SCM has not become leader");

      return isNodeReady && exitSafeMode && checkScmLeader;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  @Override
  public void waitForPipelineTobeReady(HddsProtos.ReplicationFactor factor,
                                       int timeoutInMs) throws
      TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      int openPipelineCount = scm.getPipelineManager().
          getPipelines(RatisReplicationConfig.getInstance(factor),
              Pipeline.PipelineState.OPEN).size();
      return openPipelineCount >= 1;
    }, 1000, timeoutInMs);
  }

  @Override
  public void setWaitForClusterToBeReadyTimeout(int timeoutInMs) {
    waitForClusterToBeReadyTimeout = timeoutInMs;
  }

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
  public HddsDatanodeService getHddsDatanode(DatanodeDetails dn)
      throws IOException {
    for (HddsDatanodeService service : hddsDatanodes) {
      if (service.getDatanodeDetails().equals(dn)) {
        return service;
      }
    }
    throw new IOException(
        "Not able to find datanode with datanode Id " + dn.getUuid());
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
  public OzoneClient newClient() throws IOException {
    OzoneClient client = createClient();
    clients.add(client);
    return client;
  }

  protected OzoneClient createClient() throws IOException {
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
    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf, null);

    return new StorageContainerLocationProtocolClientSideTranslatorPB(
        proxyProvider);
  }

  @Override
  public void restartStorageContainerManager(boolean waitForDatanode)
      throws TimeoutException, InterruptedException, IOException,
      AuthenticationException {
    LOG.info("Restarting SCM in cluster " + this.getClass());
    scm.stop();
    scm.join();
    scm = HddsTestUtils.getScmSimple(conf, scmConfigurator);
    scm.start();
    if (waitForDatanode) {
      waitForClusterToBeReady();
    }
  }

  @Override
  public void restartOzoneManager() throws IOException {
    stopOM(ozoneManager);
    ozoneManager.restart();
  }

  private void waitForHddsDatanodeToStop(DatanodeDetails dn)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      NodeStatus status;
      try {
        status = getStorageContainerManager()
            .getScmNodeManager().getNodeStatus(dn);
      } catch (NodeNotFoundException e) {
        return true;
      }
      if (status.equals(NodeStatus.inServiceHealthy())) {
        LOG.info("Waiting on datanode to be marked stale.");
        return false;
      } else {
        return true;
      }
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  @Override
  public void restartHddsDatanode(int i, boolean waitForDatanode)
      throws InterruptedException, TimeoutException {
    HddsDatanodeService datanodeService = hddsDatanodes.remove(i);
    stopDatanode(datanodeService);
    // ensure same ports are used across restarts.
    OzoneConfiguration config = datanodeService.getConf();
    if (waitForDatanode) {
      // wait for node to be removed from SCM healthy node list.
      waitForHddsDatanodeToStop(datanodeService.getDatanodeDetails());
    }
    HddsDatanodeService service = new HddsDatanodeService(NO_ARGS);
    service.setConfiguration(config);
    hddsDatanodes.add(i, service);
    startHddsDatanode(service);
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
  public String getClusterId() {
    return scm.getClientProtocolServer().getScmInfo().getClusterId();
  }

  @Override
  public void shutdown() {
    try {
      LOG.info("Shutting down the Mini Ozone Cluster");
      CodecTestUtil.gc();
      IOUtils.closeQuietly(clients);
      final File baseDir = new File(getBaseDir());
      stop();
      FileUtils.deleteDirectory(baseDir);
      ContainerCache.getInstance(conf).shutdownCache();
      DefaultMetricsSystem.shutdown();
      ManagedRocksObjectMetrics.INSTANCE.assertNoLeaks();
    } catch (Exception e) {
      LOG.error("Exception while shutting down the cluster.", e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping the Mini Ozone Cluster");
    stopOM(ozoneManager);
    stopDatanodes(hddsDatanodes);
    stopSCM(scm);
    stopServices(services);
  }

  private void startHddsDatanode(HddsDatanodeService datanode) {
    try {
      datanode.setCertificateClient(getCAClient());
    } catch (IOException e) {
      LOG.error("Exception while setting certificate client to DataNode.", e);
    }
    datanode.setSecretKeyClient(secretKeyClient);
    datanode.start();
  }

  @Override
  public void startHddsDatanodes() {
    hddsDatanodes.forEach(this::startHddsDatanode);
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

  public void startServices() throws Exception {
    for (Service service : services) {
      service.start(getConf());
    }
  }

  private CertificateClient getCAClient() {
    return this.caClient;
  }

  private void setCAClient(CertificateClient client) {
    this.caClient = client;
  }

  private void setSecretKeyClient(SecretKeyClient client) {
    this.secretKeyClient = client;
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

  protected static void stopOM(OzoneManager om) {
    if (om != null && om.stop()) {
      om.join();
    }
  }

  private static void stopServices(List<Service> services) {
    // stop in reverse order
    List<Service> reverse = new ArrayList<>(services);
    Collections.reverse(reverse);

    for (Service service : reverse) {
      try {
        service.stop();
        LOG.info("Stopped {}", service);
      } catch (Exception e) {
        LOG.error("Error stopping {}", service, e);
      }
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
      DatanodeStoreCache.setMiniClusterMode();
      initializeConfiguration();
      StorageContainerManager scm = null;
      OzoneManager om = null;
      List<HddsDatanodeService> hddsDatanodes = Collections.emptyList();
      try {
        scm = createAndStartSingleSCM();
        om = createAndStartSingleOM();
        hddsDatanodes = createHddsDatanodes();

        MiniOzoneClusterImpl cluster = new MiniOzoneClusterImpl(conf,
            scmConfigurator, om, scm,
            hddsDatanodes, getServices());
        cluster.startServices();

        cluster.setCAClient(certClient);
        cluster.setSecretKeyClient(secretKeyClient);
        if (startDataNodes) {
          cluster.startHddsDatanodes();
        }

        prepareForNextBuild();
        return cluster;
      } catch (Exception ex) {
        stopOM(om);
        stopServices(getServices());
        if (startDataNodes) {
          stopDatanodes(hddsDatanodes);
        }
        stopSCM(scm);
        removeConfiguration();

        LOG.warn("Unable to build MiniOzoneCluster", ex);

        if (ex instanceof IOException) {
          throw (IOException) ex;
        }
        if (ex instanceof RuntimeException) {
          throw (RuntimeException) ex;
        }
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }
    }

    protected void setClients(OzoneManager om) throws IOException {
      if (certClient != null) {
        om.setCertClient(certClient);
      }
      if (secretKeyClient != null) {
        om.setSecretKeyClient(secretKeyClient);
      }
    }

    /**
     * Initializes the configuration required for starting MiniOzoneCluster.
     */
    protected void initializeConfiguration() throws IOException {
      Path metaDir = Paths.get(path, OZONE_METADATA_SUBDIR_NAME);
      Files.createDirectories(metaDir);
      conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());

      conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          DEFAULT_RATIS_RPC_TIMEOUT_SEC, TimeUnit.SECONDS);
      SCMClientConfig scmClientConfig = conf.getObject(SCMClientConfig.class);
      // default max retry timeout set to 30s
      scmClientConfig.setMaxRetryTimeout(30 * 1000);
      conf.setFromObject(scmClientConfig);
      // In this way safemode exit will happen only when at least we have one
      // pipeline.
      conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE,
          numOfDatanodes >= 3 ? 3 : 1);
    }

    void removeConfiguration() {
      FileUtils.deleteQuietly(new File(path));
    }

    protected StorageContainerManager createAndStartSingleSCM()
        throws AuthenticationException, IOException {
      StorageContainerManager scm = createSCM();
      scm.start();
      configureScmDatanodeAddress(singletonList(scm));
      return scm;
    }

    /**
     * Creates a new StorageContainerManager instance.
     *
     * @return {@link StorageContainerManager}
     */
    protected StorageContainerManager createSCM()
        throws IOException, AuthenticationException {
      configureSCM(false);

      SCMStorageConfig scmStore = new SCMStorageConfig(conf);
      initializeScmStorage(scmStore);
      StorageContainerManager scm = HddsTestUtils.getScmSimple(conf,
          scmConfigurator);
      HealthyPipelineSafeModeRule rule = SafeModeRuleFactory.getInstance()
          .getSafeModeRule(HealthyPipelineSafeModeRule.class);
      if (rule != null) {
        // Set threshold to wait for safe mode exit - this is needed since a
        // pipeline is marked open only after leader election.
        rule.setHealthyPipelineThresholdCount(numOfDatanodes / 3);
      }
      return scm;
    }

    protected void initializeScmStorage(SCMStorageConfig scmStore)
        throws IOException {
      if (scmStore.getState() == StorageState.INITIALIZED) {
        return;
      }
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmId);
      scmStore.initialize();
      scmStore.setSCMHAFlag(true);
      scmStore.persistCurrentState();
      SCMRatisServerImpl.initialize(clusterId, scmId,
          SCMHANodeDetails.loadSCMHAConfig(conf, scmStore)
              .getLocalNodeDetails(), conf);

    }

    void initializeOmStorage(OMStorage omStorage) throws IOException {
      if (omStorage.getState() == StorageState.INITIALIZED) {
        return;
      }
      omStorage.setClusterId(clusterId);
      omStorage.setOmId(omId);
      // Initialize ozone certificate client if security is enabled.
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        OzoneManager.initializeSecurity(conf, omStorage, scmId);
      }
      omStorage.initialize();
    }

    protected OzoneManager createAndStartSingleOM() throws AuthenticationException, IOException {
      OzoneManager om = createOM();
      setClients(om);
      om.start();
      return om;
    }

    /**
     * Creates a new OzoneManager instance.
     *
     * @return {@link OzoneManager}
     */
    protected OzoneManager createOM()
        throws IOException, AuthenticationException {
      configureOM(false);
      OMStorage omStore = new OMStorage(conf);
      initializeOmStorage(omStore);
      return OzoneManager.createOm(conf);
    }

    private String getSCMAddresses(List<StorageContainerManager> scms) {
      StringBuilder stringBuilder = new StringBuilder();
      Iterator<StorageContainerManager> iter = scms.iterator();

      while (iter.hasNext()) {
        StorageContainerManager scm = iter.next();
        stringBuilder.append(scm.getDatanodeRpcAddress().getHostString())
            .append(':')
            .append(scm.getDatanodeRpcAddress().getPort());
        if (iter.hasNext()) {
          stringBuilder.append(',');
        }

      }
      return stringBuilder.toString();
    }

    protected void configureScmDatanodeAddress(List<StorageContainerManager> scms) {
      conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, getSCMAddresses(scms));
    }

    /**
     * Creates HddsDatanodeService(s) instance.
     *
     * @return List of HddsDatanodeService
     */
    protected List<HddsDatanodeService> createHddsDatanodes()
        throws IOException {
      List<HddsDatanodeService> hddsDatanodes = new ArrayList<>();

      for (int i = 0; i < numOfDatanodes; i++) {
        OzoneConfiguration dnConf = dnFactory.apply(conf);

        HddsDatanodeService datanode = new HddsDatanodeService(NO_ARGS);
        datanode.setConfiguration(dnConf);
        hddsDatanodes.add(datanode);
      }
      return hddsDatanodes;
    }

    protected void configureSCM(boolean isHA) throws IOException {
      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
          localhostWithFreePort());
      conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
          localhostWithFreePort());
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
          localhostWithFreePort());
      conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY,
          localhostWithFreePort());
      conf.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
          "3s");
      conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, getFreePort());
      conf.setInt(ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, getFreePort());
      conf.setIfUnset(ScmConfigKeys.OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT, "1s");

      Path scmMetaDir = Paths.get(path, SCM_SUBDIR_NAME);
      Files.createDirectories(scmMetaDir.resolve(DATA_SUBDIR_NAME));

      // Extra configs for non-HA SCM
      if (!isHA) {
        // e.g. when path is /var/lib/hadoop-ozone
        // ozone.scm.ha.ratis.storage.dir = /var/lib/hadoop-ozone/scm/ratis
        conf.setIfUnset(ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR,
            scmMetaDir.resolve(RATIS_SUBDIR_NAME).toString());

        // ozone.scm.ha.ratis.snapshot.dir = /var/lib/hadoop-ozone/scm/ozone-metadata/snapshot
        conf.setIfUnset(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_DIR,
            scmMetaDir.resolve(OZONE_METADATA_SUBDIR_NAME).resolve(OZONE_RATIS_SNAPSHOT_DIR).toString());

        // ozone.scm.db.dirs = /var/lib/hadoop-ozone/scm/data
        conf.setIfUnset(ScmConfigKeys.OZONE_SCM_DB_DIRS,
            scmMetaDir.resolve(DATA_SUBDIR_NAME).toString());

        // ozone.http.basedir = /var/lib/hadoop-ozone/scm/ozone-metadata/webserver
        conf.setIfUnset(OzoneConfigKeys.OZONE_HTTP_BASEDIR,
            scmMetaDir.resolve(OZONE_METADATA_SUBDIR_NAME) + SERVER_DIR);
      }
    }

    private void configureOM(boolean isHA) throws IOException {
      conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, localhostWithFreePort());
      conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, localhostWithFreePort());
      conf.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY,
          localhostWithFreePort());
      conf.setInt(OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, getFreePort());

      Path omMetaDir = Paths.get(path, OM_SUBDIR_NAME);
      Files.createDirectories(omMetaDir.resolve(DATA_SUBDIR_NAME));

      // Extra configs for non-HA OM
      if (!isHA) {
        // e.g. when path is /var/lib/hadoop-ozone
        // ozone.om.ratis.storage.dir = /var/lib/hadoop-ozone/om/ratis
        conf.setIfUnset(OMConfigKeys.OZONE_OM_RATIS_STORAGE_DIR,
            omMetaDir.resolve(RATIS_SUBDIR_NAME).toString());

        // ozone.om.ratis.snapshot.dir = /var/lib/hadoop-ozone/om/ozone-metadata/snapshot
        conf.setIfUnset(OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_DIR,
            omMetaDir.resolve(OZONE_METADATA_SUBDIR_NAME).resolve(OZONE_RATIS_SNAPSHOT_DIR).toString());

        // ozone.om.db.dirs = /var/lib/hadoop-ozone/om/data
        conf.setIfUnset(OMConfigKeys.OZONE_OM_DB_DIRS,
            omMetaDir.resolve(DATA_SUBDIR_NAME).toString());

        // ozone.om.snapshot.diff.db.dir = /var/lib/hadoop-ozone/om/ozone-metadata
        // actual dir would be /var/lib/hadoop-ozone/om/ozone-metadata/db.snapdiff
        conf.setIfUnset(OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DB_DIR,
            omMetaDir.resolve(OZONE_METADATA_SUBDIR_NAME).toString());

        // ozone.http.basedir = /var/lib/hadoop-ozone/om/ozone-metadata/webserver
        conf.setIfUnset(OzoneConfigKeys.OZONE_HTTP_BASEDIR,
            omMetaDir.resolve(OZONE_METADATA_SUBDIR_NAME) + SERVER_DIR);
      }
    }

  }
}
