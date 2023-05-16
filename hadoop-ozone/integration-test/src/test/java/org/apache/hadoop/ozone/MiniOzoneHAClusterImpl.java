/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.ConfigurationTarget;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.CheckedConsumer;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION;
import static org.apache.hadoop.ozone.MiniOzoneCluster.PortAllocator.getFreePort;
import static org.apache.hadoop.ozone.MiniOzoneCluster.PortAllocator.localhostWithFreePort;
import static org.apache.hadoop.ozone.om.OmUpgradeConfig.ConfigStrings.OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION;

/**
 * MiniOzoneHAClusterImpl creates a complete in-process Ozone cluster
 * with OM HA and SCM HA suitable for running tests.
 * The cluster consists of a set of
 * OzoneManagers, StorageContainerManagers and multiple DataNodes.
 */
public class MiniOzoneHAClusterImpl extends MiniOzoneClusterImpl {

  public static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneHAClusterImpl.class);

  private final OMHAService omhaService;
  private final SCMHAService scmhaService;

  private final String clusterMetaPath;

  private int waitForClusterToBeReadyTimeout = 120000; // 2 min

  private static final int RATIS_RPC_TIMEOUT = 1000; // 1 second
  public static final int NODE_FAILURE_TIMEOUT = 2000; // 2 seconds

  /**
   * Creates a new MiniOzoneCluster.
   *
   * @throws IOException if there is an I/O error
   */
  public MiniOzoneHAClusterImpl(
      OzoneConfiguration conf,
      SCMConfigurator scmConfigurator,
      OMHAService omhaService,
      SCMHAService scmhaService,
      List<HddsDatanodeService> hddsDatanodes,
      String clusterPath,
      ReconServer reconServer) {
    super(conf, scmConfigurator, hddsDatanodes, reconServer);
    this.omhaService = omhaService;
    this.scmhaService = scmhaService;
    this.clusterMetaPath = clusterPath;
  }

  @Override
  public String getOMServiceId() {
    return omhaService.getServiceId();
  }

  @Override
  public String getSCMServiceId() {
    return scmhaService.getServiceId();
  }

  /**
   * Returns the first OzoneManager from the list.
   * @return
   */
  @Override
  public OzoneManager getOzoneManager() {
    return this.omhaService.getServices().get(0);
  }

  @Override
  protected OzoneClient createClient() throws IOException {
    String omServiceId = omhaService.getServiceId();
    if (omServiceId == null) {
      // Non-HA cluster.
      return OzoneClientFactory.getRpcClient(getConf());
    } else {
      // HA cluster
      return OzoneClientFactory.getRpcClient(omServiceId, getConf());
    }
  }

  public boolean isOMActive(String omNodeId) {
    return omhaService.isServiceActive(omNodeId);
  }

  public Iterator<StorageContainerManager> getInactiveSCM() {
    return scmhaService.inactiveServices();
  }

  public StorageContainerManager getSCM(String scmNodeId) {
    return this.scmhaService.getServiceById(scmNodeId);
  }

  public OzoneManager getOzoneManager(int index) {
    return this.omhaService.getServiceByIndex(index);
  }

  public OzoneManager getOzoneManager(String omNodeId) {
    return this.omhaService.getServiceById(omNodeId);
  }

  public List<OzoneManager> getOzoneManagersList() {
    return omhaService.getServices();
  }

  public List<StorageContainerManager> getStorageContainerManagersList() {
    return scmhaService.getServices();
  }

  public StorageContainerManager getStorageContainerManager(int index) {
    return this.scmhaService.getServiceByIndex(index);
  }

  private OzoneManager getOMLeader(boolean waitForLeaderElection)
      throws TimeoutException, InterruptedException {
    if (waitForLeaderElection) {
      final OzoneManager[] om = new OzoneManager[1];
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          om[0] = getOMLeader();
          return om[0] != null;
        }
      }, 200, waitForClusterToBeReadyTimeout);
      return om[0];
    } else {
      return getOMLeader();
    }
  }

  /**
   * Get OzoneManager leader object.
   * @return OzoneManager object, null if there isn't one or more than one
   */
  public OzoneManager getOMLeader() {
    OzoneManager res = null;
    for (OzoneManager ozoneManager : this.omhaService.getActiveServices()) {
      if (ozoneManager.isLeaderReady()) {
        if (res != null) {
          // Found more than one leader
          // Return null, expect the caller to retry in a while
          return null;
        }
        // Found a leader
        res = ozoneManager;
      }
    }
    return res;
  }

  /**
   * Start a previously inactive OM.
   */
  public void startInactiveOM(String omNodeID) throws IOException {
    omhaService.startInactiveService(omNodeID, OzoneManager::start);
  }

  /**
   * Start a previously inactive SCM.
   */
  public void startInactiveSCM(String scmNodeId) throws IOException {
    scmhaService
        .startInactiveService(scmNodeId, StorageContainerManager::start);
  }

  @Override
  public void restartOzoneManager() throws IOException {
    for (OzoneManager ozoneManager : this.omhaService.getServices()) {
      ozoneManager.stop();
      ozoneManager.restart();
    }
  }

  public void shutdownOzoneManager(OzoneManager ozoneManager) {
    LOG.info("Shutting down OzoneManager " + ozoneManager.getOMNodeId());

    ozoneManager.stop();
  }

  public void restartOzoneManager(OzoneManager ozoneManager, boolean waitForOM)
      throws IOException, TimeoutException, InterruptedException {
    LOG.info("Restarting OzoneManager " + ozoneManager.getOMNodeId());
    ozoneManager.restart();

    if (waitForOM) {
      GenericTestUtils.waitFor(ozoneManager::isRunning,
          1000, waitForClusterToBeReadyTimeout);
    }
  }

  public void shutdownStorageContainerManager(StorageContainerManager scm) {
    LOG.info("Shutting down StorageContainerManager " + scm.getScmId());

    scm.stop();
    scmhaService.removeInstance(scm);
  }

  public StorageContainerManager restartStorageContainerManager(
      StorageContainerManager scm, boolean waitForSCM)
      throws IOException, TimeoutException,
      InterruptedException, AuthenticationException {
    LOG.info("Restarting SCM in cluster " + this.getClass());
    scmhaService.removeInstance(scm);
    OzoneConfiguration scmConf = scm.getConfiguration();
    shutdownStorageContainerManager(scm);
    scm.join();
    scm = HddsTestUtils.getScmSimple(scmConf, getSCMConfigurator());
    scmhaService.addInstance(scm, true);
    scm.start();
    if (waitForSCM) {
      waitForClusterToBeReady();
    }
    return scm;
  }

  @Override
  public String getClusterId() {
    return scmhaService.getServices().get(0)
        .getClientProtocolServer().getScmInfo().getClusterId();
  }

  public StorageContainerManager getActiveSCM() {
    for (StorageContainerManager scm : scmhaService.getServices()) {
      if (scm.checkLeader()) {
        return scm;
      }
    }
    return null;
  }

  public void waitForSCMToBeReady()
      throws TimeoutException, InterruptedException  {
    GenericTestUtils.waitFor(() -> {
      for (StorageContainerManager scm : scmhaService.getServices()) {
        if (scm.checkLeader()) {
          return true;
        }
      }
      return false;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  @Override
  public void stop() {
    for (OzoneManager ozoneManager : this.omhaService.getServices()) {
      if (ozoneManager != null) {
        LOG.info("Stopping the OzoneManager {}", ozoneManager.getOMNodeId());
        ozoneManager.stop();
        ozoneManager.join();
      }
    }

    for (StorageContainerManager scm : this.scmhaService.getServices()) {
      if (scm != null) {
        LOG.info("Stopping the StorageContainerManager {}", scm.getScmId());
        scm.stop();
        scm.join();
      }
    }
    super.stop();
  }

  public void stopOzoneManager(int index) {
    OzoneManager om = omhaService.getServices().get(index);
    om.stop();
    om.join();
    omhaService.deactivate(om);
  }

  public void stopOzoneManager(String omNodeId) {
    OzoneManager om = omhaService.getServiceById(omNodeId);
    om.stop();
    om.join();
    omhaService.deactivate(om);
  }

  private static void configureOMPorts(ConfigurationTarget conf,
      String omServiceId, String omNodeId) {

    String omAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpsAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId);
    String omRatisPortKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

    conf.set(omAddrKey, localhostWithFreePort());
    conf.set(omHttpAddrKey, localhostWithFreePort());
    conf.set(omHttpsAddrKey, localhostWithFreePort());
    conf.setInt(omRatisPortKey, getFreePort());
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder extends MiniOzoneClusterImpl.Builder {

    private static final String OM_NODE_ID_PREFIX = "omNode-";
    private List<OzoneManager> activeOMs = new ArrayList<>();
    private List<OzoneManager> inactiveOMs = new ArrayList<>();

    private static final String SCM_NODE_ID_PREFIX = "scmNode-";
    private List<StorageContainerManager> activeSCMs = new ArrayList<>();
    private List<StorageContainerManager> inactiveSCMs = new ArrayList<>();

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
      if (numOfActiveOMs > numOfOMs) {
        throw new IllegalArgumentException("Number of active OMs cannot be " +
            "more than the total number of OMs");
      }

      // If num of ActiveOMs is not set, set it to numOfOMs.
      if (numOfActiveOMs == ACTIVE_OMS_NOT_SET) {
        numOfActiveOMs = numOfOMs;
      }

      // If num of SCMs it not set, set it to 1.
      if (numOfSCMs == 0) {
        numOfSCMs = 1;
      }

      // If num of ActiveSCMs is not set, set it to numOfSCMs.
      if (numOfActiveSCMs == ACTIVE_SCMS_NOT_SET) {
        numOfActiveSCMs = numOfSCMs;
      }

      DefaultMetricsSystem.setMiniClusterMode(true);
      DatanodeStoreCache.setMiniClusterMode();
      initializeConfiguration();
      initOMRatisConf();
      SCMHAService scmService;
      OMHAService omService;
      ReconServer reconServer = null;
      try {
        scmService = createSCMService();
        omService = createOMService();
        if (includeRecon) {
          configureRecon();
          reconServer = new ReconServer();
          reconServer.execute(new String[] {});
        }
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(
          scmService.getActiveServices(), reconServer);

      MiniOzoneHAClusterImpl cluster = new MiniOzoneHAClusterImpl(conf,
          scmConfigurator, omService, scmService, hddsDatanodes, path,
          reconServer);

      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }

    protected void initOMRatisConf() {
      conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
      conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numOfOmHandlers);

      // If test change the following config values we will respect,
      // otherwise we will set lower timeout values.
      long defaultDuration = OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT
          .getDuration();
      long curRatisRpcTimeout = conf.getTimeDuration(
          OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          defaultDuration, TimeUnit.MILLISECONDS);
      conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
          defaultDuration == curRatisRpcTimeout ?
              RATIS_RPC_TIMEOUT : curRatisRpcTimeout, TimeUnit.MILLISECONDS);

      long defaultNodeFailureTimeout =
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT.
              getDuration();
      long curNodeFailureTimeout = conf.getTimeDuration(
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
          defaultNodeFailureTimeout,
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT.
              getUnit());
      conf.setTimeDuration(
          OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
          curNodeFailureTimeout == defaultNodeFailureTimeout ?
              NODE_FAILURE_TIMEOUT : curNodeFailureTimeout,
          TimeUnit.MILLISECONDS);
    }

    /**
     * Start OM service with multiple OMs.
     */
    protected OMHAService createOMService() throws IOException,
        AuthenticationException {
      if (omServiceId == null) {
        OzoneManager om = createOM();
        om.start();
        return new OMHAService(singletonList(om), null, null);
      }

      List<OzoneManager> omList = Lists.newArrayList();

      int retryCount = 0;

      while (true) {
        try {
          initOMHAConfig();

          for (int i = 1; i <= numOfOMs; i++) {
            // Set nodeId
            String nodeId = OM_NODE_ID_PREFIX + i;
            OzoneConfiguration config = new OzoneConfiguration(conf);
            config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);
            // Set the OM http(s) address to null so that the cluster picks
            // up the address set with service ID and node ID in initHAConfig
            config.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "");
            config.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "");

            // Set metadata/DB dir base path
            String metaDirPath = path + "/" + nodeId;
            config.set(OZONE_METADATA_DIRS, metaDirPath);

            // Set non standard layout version if needed.
            omLayoutVersion.ifPresent(integer ->
                config.set(OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION,
                    String.valueOf(integer)));

            OzoneManager.omInit(config);
            OzoneManager om = OzoneManager.createOm(config);
            if (certClient != null) {
              om.setCertClient(certClient);
            }
            omList.add(om);

            if (i <= numOfActiveOMs) {
              om.start();
              activeOMs.add(om);
              LOG.info("Started OzoneManager {} RPC server at {}", nodeId,
                  om.getOmRpcServerAddr());
            } else {
              inactiveOMs.add(om);
              LOG.info("Initialized OzoneManager at {}. This OM is currently "
                  + "inactive (not running).", om.getOmRpcServerAddr());
            }
          }

          break;
        } catch (BindException e) {
          for (OzoneManager om : omList) {
            om.stop();
            om.join();
            LOG.info("Stopping OzoneManager server at {}",
                om.getOmRpcServerAddr());
          }
          omList.clear();
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount, e);
        }
      }
      return new OMHAService(activeOMs, inactiveOMs, omServiceId);
    }

    /**
     * Start OM service with multiple OMs.
     */
    protected SCMHAService createSCMService()
        throws IOException, AuthenticationException {
      if (scmServiceId == null) {
        StorageContainerManager scm = createSCM();
        scm.start();
        return new SCMHAService(singletonList(scm), null, null);
      }

      List<StorageContainerManager> scmList = Lists.newArrayList();

      int retryCount = 0;

      while (true) {
        try {
          initSCMHAConfig();

          for (int i = 1; i <= numOfSCMs; i++) {
            // Set nodeId
            String nodeId = SCM_NODE_ID_PREFIX + i;
            String metaDirPath = path + "/" + nodeId;
            OzoneConfiguration scmConfig = new OzoneConfiguration(conf);
            scmConfig.set(OZONE_METADATA_DIRS, metaDirPath);
            scmConfig.set(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY, nodeId);
            scmConfig.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);

            scmLayoutVersion.ifPresent(integer ->
                scmConfig.set(HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
                    String.valueOf(integer)));

            configureSCM();
            if (i == 1) {
              StorageContainerManager.scmInit(scmConfig, clusterId);
            } else {
              StorageContainerManager.scmBootstrap(scmConfig);
            }
            StorageContainerManager scm =
                HddsTestUtils.getScmSimple(scmConfig, scmConfigurator);
            HealthyPipelineSafeModeRule rule =
                scm.getScmSafeModeManager().getHealthyPipelineSafeModeRule();
            if (rule != null) {
              // Set threshold to wait for safe mode exit -
              // this is needed since a pipeline is marked open only after
              // leader election.
              rule.setHealthyPipelineThresholdCount(numOfDatanodes / 3);
            }
            scmList.add(scm);

            if (i <= numOfActiveSCMs) {
              scm.start();
              activeSCMs.add(scm);
              LOG.info("Started SCM RPC server at {}",
                  scm.getClientRpcAddress());
            } else {
              inactiveSCMs.add(scm);
              LOG.info("Intialized SCM at {}. This SCM is currently "
                  + "inactive (not running).", scm.getClientRpcAddress());
            }
          }
          break;
        } catch (BindException e) {
          for (StorageContainerManager scm : scmList) {
            scm.stop();
            scm.join();
            LOG.info("Stopping StorageContainerManager server at {}",
                scm.getClientRpcAddress());
          }
          scmList.clear();
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount, e);
        }
      }

      return new SCMHAService(activeSCMs, inactiveSCMs, scmServiceId);
    }

    /**
     * Initialize HA related configurations.
     */
    private void initSCMHAConfig() {
      // Set configurations required for starting OM HA service, because that
      // is the serviceID being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_OM_SERVICE_IDS_KEY, in this
      // way in OM start it uses internal service id to find it's service id.
      conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
      conf.set(ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID, scmServiceId);
      String scmNodesKey = ConfUtils.addKeySuffixes(
          ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
      StringBuilder scmNodesKeyValue = new StringBuilder();
      StringBuilder scmNames = new StringBuilder();

      for (int i = 1; i <= numOfSCMs; i++) {
        String scmNodeId = SCM_NODE_ID_PREFIX + i;
        scmNodesKeyValue.append(",").append(scmNodeId);

        String scmAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmHttpAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmHttpsAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmRatisPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, scmServiceId, scmNodeId);
        String dnPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
            scmServiceId, scmNodeId);
        String blockClientKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
            scmServiceId, scmNodeId);
        String ssClientKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
            scmServiceId, scmNodeId);
        String scmGrpcPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, scmServiceId, scmNodeId);

        conf.set(scmAddrKey, "127.0.0.1");
        conf.set(scmHttpAddrKey, localhostWithFreePort());
        conf.set(scmHttpsAddrKey, localhostWithFreePort());

        int ratisPort = getFreePort();
        conf.setInt(scmRatisPortKey, ratisPort);
        //conf.setInt("ozone.scm.ha.ratis.bind.port", ratisPort);

        int dnPort = getFreePort();
        conf.set(dnPortKey, "127.0.0.1:" + dnPort);
        scmNames.append(",localhost:").append(dnPort);

        conf.set(ssClientKey, localhostWithFreePort());
        conf.setInt(scmGrpcPortKey, getFreePort());

        String blockAddress = localhostWithFreePort();
        conf.set(blockClientKey, blockAddress);
        conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
            blockAddress);
      }

      conf.set(scmNodesKey, scmNodesKeyValue.substring(1));
      conf.set(ScmConfigKeys.OZONE_SCM_NAMES, scmNames.substring(1));
    }

    /**
     * Initialize HA related configurations.
     */
    private void initOMHAConfig() {
      // Set configurations required for starting OM HA service, because that
      // is the serviceID being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_OM_SERVICE_IDS_KEY, in this
      // way in OM start it uses internal service id to find it's service id.
      conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
      conf.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, omServiceId);
      String omNodesKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
      List<String> omNodeIds = new ArrayList<>();

      for (int i = 1; i <= numOfOMs; i++) {
        String omNodeId = OM_NODE_ID_PREFIX + i;
        omNodeIds.add(omNodeId);

        configureOMPorts(conf, omServiceId, omNodeId);
      }

      conf.set(omNodesKey, String.join(",", omNodeIds));
    }
  }

  /**
   * Bootstrap new OM by updating existing OM configs.
   */
  public void bootstrapOzoneManager(String omNodeId) throws Exception {
    bootstrapOzoneManager(omNodeId, true, false);
  }

  /**
   * Bootstrap new OM and add to existing OM HA service ring.
   * @param omNodeId nodeId of new OM
   * @param updateConfigs if true, update the old OM configs with new node
   *                      information
   * @param force if true, start new OM with FORCE_BOOTSTRAP option.
   *              Otherwise, start new OM with BOOTSTRAP option.
   */
  public void bootstrapOzoneManager(String omNodeId,
      boolean updateConfigs, boolean force) throws Exception {

    // Set testReloadConfigFlag to true so that
    // OzoneManager#reloadConfiguration does not reload config as it will
    // return the default configurations.
    OzoneManager.setTestReloadConfigFlag(true);

    int retryCount = 0;
    OzoneManager om = null;

    OzoneManager omLeader = getOMLeader(true);
    long leaderSnapshotIndex = omLeader.getRatisSnapshotIndex();

    while (true) {
      try {
        OzoneConfiguration newConf = addNewOMToConfig(getOMServiceId(),
            omNodeId);

        if (updateConfigs) {
          updateOMConfigs(newConf);
        }

        om = bootstrapNewOM(omNodeId, newConf, force);

        LOG.info("Bootstrapped OzoneManager {} RPC server at {}", omNodeId,
            om.getOmRpcServerAddr());

        // Add new OMs to cluster's in memory map and update existing OMs conf.
        setConf(newConf);

        break;
      } catch (IOException e) {
        // Existing OM config could have been updated with new conf. Reset it.
        for (OzoneManager existingOM : omhaService.getServices()) {
          existingOM.setConfiguration(getConf());
        }
        if (e instanceof BindException ||
            e.getCause() instanceof BindException) {
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount, e);
        } else {
          throw e;
        }
      }
    }

    waitForBootstrappedNodeToBeReady(om, leaderSnapshotIndex);
    if (updateConfigs) {
      waitForConfigUpdateOnActiveOMs(omNodeId);
    }
  }

  /**
   * Set the configs for new OMs.
   */
  private OzoneConfiguration addNewOMToConfig(String omServiceId,
      String omNodeId) {

    OzoneConfiguration newConf = new OzoneConfiguration(getConf());
    configureOMPorts(newConf, omServiceId, omNodeId);

    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    newConf.set(omNodesKey, newConf.get(omNodesKey) + "," + omNodeId);

    return newConf;
  }

  /**
   * Update the configurations of the given list of OMs.
   */
  public void updateOMConfigs(OzoneConfiguration newConf) {
    for (OzoneManager om : omhaService.getActiveServices()) {
      om.setConfiguration(newConf);
    }
  }

  /**
   * Start a new OM in Bootstrap mode. Configs (address and ports) for the new
   * OM must already be set in the newConf.
   */
  private OzoneManager bootstrapNewOM(String nodeId, OzoneConfiguration newConf,
      boolean force) throws IOException, AuthenticationException {

    OzoneConfiguration config = new OzoneConfiguration(newConf);

    // For bootstrapping node, set the nodeId config also.
    config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);

    // Set metadata/DB dir base path
    String metaDirPath = clusterMetaPath + "/" + nodeId;
    config.set(OZONE_METADATA_DIRS, metaDirPath);

    OzoneManager.omInit(config);
    OzoneManager om;

    if (force) {
      om = OzoneManager.createOm(config,
          OzoneManager.StartupOption.FORCE_BOOTSTRAP);
    } else {
      om = OzoneManager.createOm(config, OzoneManager.StartupOption.BOOTSTRAP);
    }

    ExitManagerForOM exitManager = new ExitManagerForOM(this, nodeId);
    om.setExitManagerForTesting(exitManager);
    omhaService.addInstance(om, false);
    startInactiveOM(nodeId);

    return om;
  }

  /**
   * Wait for AddOM command to execute on all OMs.
   */
  private void waitForBootstrappedNodeToBeReady(OzoneManager newOM,
      long leaderSnapshotIndex) throws Exception {
    // Wait for bootstrapped nodes to catch up with others
    GenericTestUtils.waitFor(() -> {
      try {
        if (newOM.getRatisSnapshotIndex() >= leaderSnapshotIndex) {
          return true;
        }
      } catch (IOException e) {
        return false;
      }
      return false;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  private void waitForConfigUpdateOnActiveOMs(String newOMNodeId)
      throws Exception {
    OzoneManager newOMNode = omhaService.getServiceById(newOMNodeId);
    OzoneManagerRatisServer newOMRatisServer = newOMNode.getOmRatisServer();
    GenericTestUtils.waitFor(() -> {
      // Each existing active OM should contain the new OM in its peerList.
      // Also, the new OM should contain each existing active OM in it's OM
      // peer list and RatisServer peerList.
      for (OzoneManager om : omhaService.getActiveServices()) {
        if (!om.doesPeerExist(newOMNodeId)) {
          return false;
        }
        if (!newOMNode.doesPeerExist(om.getOMNodeId())) {
          return false;
        }
        if (!newOMRatisServer.doesPeerExist(om.getOMNodeId())) {
          return false;
        }
      }
      return true;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  public void setupExitManagerForTesting() {
    for (OzoneManager om : omhaService.getServices()) {
      om.setExitManagerForTesting(new ExitManagerForOM(this, om.getOMNodeId()));
    }
  }

  /**
   * MiniOzoneHAService is a helper class used for both SCM and OM HA.
   * This class keeps track of active and inactive OM/SCM services
   * @param <Type>
   */
  static class MiniOzoneHAService<Type> {
    private Map<String, Type> serviceMap;
    private List<Type> services;
    private String serviceId;
    private String serviceName;

    // Active services s denote OM/SCM services which are up and running
    private List<Type> activeServices;
    private List<Type> inactiveServices;

    // Function to extract the Id from service
    private Function<Type, String> serviceIdProvider;

    MiniOzoneHAService(String name, List<Type> activeList,
        List<Type> inactiveList, String serviceId,
        Function<Type, String> idProvider) {
      this.serviceName = name;
      this.serviceMap = Maps.newHashMap();
      this.serviceIdProvider = idProvider;
      if (activeList != null) {
        for (Type service : activeList) {
          this.serviceMap.put(idProvider.apply(service), service);
        }
      }
      if (inactiveList != null) {
        for (Type service : inactiveList) {
          this.serviceMap.put(idProvider.apply(service), service);
        }
      }
      this.services = new ArrayList<>(serviceMap.values());
      this.activeServices = activeList;
      this.inactiveServices = inactiveList;
      this.serviceId = serviceId;

      // If the serviceID is null, then this should be a non-HA cluster.
      if (serviceId == null) {
        Preconditions.checkArgument(services.size() <= 1);
      }
    }

    public String getServiceId() {
      return serviceId;
    }

    public List<Type> getServices() {
      return services;
    }

    public List<Type> getActiveServices() {
      return activeServices;
    }

    public boolean removeInstance(Type t) {
      boolean result =  services.remove(t);
      serviceMap.remove(serviceIdProvider.apply(t));
      activeServices.remove(t);
      inactiveServices.remove(t);
      return result;
    }

    public void addInstance(Type t, boolean isActive) {
      services.add(t);
      serviceMap.put(serviceIdProvider.apply(t), t);
      if (isActive) {
        activeServices.add(t);
      } else {
        inactiveServices.add(t);
      }
    }

    public void activate(Type t) {
      activeServices.add(t);
      inactiveServices.remove(t);
    }

    public void deactivate(Type t) {
      activeServices.remove(t);
      inactiveServices.add(t);
    }

    public boolean isServiceActive(String id) {
      return activeServices.contains(serviceMap.get(id));
    }

    public Iterator<Type> inactiveServices() {
      return new ArrayList<>(inactiveServices).iterator();
    }

    public Type getServiceByIndex(int index) {
      return this.services.get(index);
    }

    public Type getServiceById(String id) {
      return this.serviceMap.get(id);
    }

    public void startInactiveService(String id,
        CheckedConsumer<Type, IOException> serviceStarter) throws IOException {
      Type service = serviceMap.get(id);
      if (!inactiveServices.contains(service)) {
        throw new IOException(serviceName + " is already active.");
      } else {
        serviceStarter.execute(service);
        activeServices.add(service);
        inactiveServices.remove(service);
      }
    }
  }

  static class OMHAService extends MiniOzoneHAService<OzoneManager> {
    OMHAService(List<OzoneManager> activeList, List<OzoneManager> inactiveList,
        String serviceId) {
      super("OM", activeList, inactiveList, serviceId,
          OzoneManager::getOMNodeId);
    }
  }

  static class SCMHAService extends
      MiniOzoneHAService<StorageContainerManager> {
    SCMHAService(List<StorageContainerManager> activeList,
        List<StorageContainerManager> inactiveList,
        String serviceId) {
      super("SCM", activeList, inactiveList, serviceId,
          StorageContainerManager::getSCMNodeId);
    }
  }

  public List<StorageContainerManager> getStorageContainerManagers() {
    return new ArrayList<>(this.scmhaService.getServices());
  }

  public StorageContainerManager getStorageContainerManager() {
    return getStorageContainerManagers().get(0);
  }

  private static final class ExitManagerForOM extends ExitManager {

    private MiniOzoneHAClusterImpl cluster;
    private String omNodeId;

    private ExitManagerForOM(MiniOzoneHAClusterImpl cluster, String nodeId) {
      this.cluster = cluster;
      this.omNodeId = nodeId;
    }

    @Override
    public void exitSystem(int status, String message, Throwable throwable,
        Logger log) throws IOException {
      LOG.error(omNodeId + " - System Exit: " + message, throwable);
      cluster.stopOzoneManager(omNodeId);
      throw new IOException(throwable);
    }

    @Override
    public void exitSystem(int status, String message, Logger log)
        throws IOException {
      LOG.error(omNodeId + " - System Exit: " + message);
      cluster.stopOzoneManager(omNodeId);
      throw new IOException(message);
    }
  }

}
