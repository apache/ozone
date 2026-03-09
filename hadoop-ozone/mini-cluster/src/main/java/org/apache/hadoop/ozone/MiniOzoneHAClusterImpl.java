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
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.getFreePort;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.ConfigurationTarget;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.safemode.SafeModeRuleFactory;
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
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MiniOzoneHAClusterImpl creates a complete in-process Ozone cluster
 * with OM HA and SCM HA suitable for running tests.
 * The cluster consists of a set of
 * OzoneManagers, StorageContainerManagers and multiple DataNodes.
 */
public class MiniOzoneHAClusterImpl extends MiniOzoneClusterImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneHAClusterImpl.class);

  private final OMHAService omhaService;
  private final SCMHAService scmhaService;

  private final String clusterMetaPath;

  private int waitForClusterToBeReadyTimeout = 120000; // 2 min

  private static final int RATIS_RPC_TIMEOUT = 1000; // 1 second
  public static final int NODE_FAILURE_TIMEOUT = 2000; // 2 seconds

  public MiniOzoneHAClusterImpl(
      OzoneConfiguration conf,
      SCMConfigurator scmConfigurator,
      OMHAService omhaService,
      SCMHAService scmhaService,
      List<HddsDatanodeService> hddsDatanodes,
      String clusterPath,
      List<Service> services) {
    super(conf, scmConfigurator, hddsDatanodes, services);
    this.omhaService = omhaService;
    this.scmhaService = scmhaService;
    this.clusterMetaPath = clusterPath;
  }

  /**
   * Returns the first OzoneManager from the list.
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

  public boolean isSCMActive(String scmNodeId) {
    return scmhaService.isServiceActive(scmNodeId);
  }

  public Iterator<StorageContainerManager> getInactiveSCM() {
    return scmhaService.inactiveServices();
  }

  public Iterator<OzoneManager> getInactiveOM() {
    return omhaService.inactiveServices();
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

  public void restartOzoneManagersWithConfigCustomizer(Consumer<OzoneConfiguration> configCustomizer)
      throws IOException, TimeoutException, InterruptedException {
    List<OzoneManager> toRestart = new ArrayList<>();
    for (OzoneManager om : getOzoneManagersList()) {
      OzoneConfiguration configuration = new OzoneConfiguration(om.getConfiguration());
      if (configCustomizer != null) {
        configCustomizer.accept(configuration);
      }
      om.setConfiguration(configuration);
      if (om.isRunning()) {
        toRestart.add(om);
      }
    }
    for (OzoneManager om : toRestart) {
      if (!om.stop()) {
        continue;
      }
      om.join();
      om.restart();
      GenericTestUtils.waitFor(om::isRunning, 1000, 30000);
    }
    waitForLeaderOM();
  }

  public List<StorageContainerManager> getStorageContainerManagersList() {
    return scmhaService.getServices();
  }

  public StorageContainerManager getStorageContainerManager(int index) {
    return this.scmhaService.getServiceByIndex(index);
  }

  public StorageContainerManager getScmLeader() {
    return getStorageContainerManagers().stream()
        .filter(StorageContainerManager::checkLeader)
        .findFirst().orElse(null);
  }

  public StorageContainerManager getScmLeader(boolean waitForLeaderElection)
      throws TimeoutException, InterruptedException {
    if (waitForLeaderElection) {
      final StorageContainerManager[] scm = new StorageContainerManager[1];
      GenericTestUtils.waitFor(() -> {
        scm[0] = getScmLeader();
        return scm[0] != null;
      }, 200, waitForClusterToBeReadyTimeout);
      return scm[0];
    } else {
      return getScmLeader();
    }
  }

  public OzoneManager waitForLeaderOM()
      throws TimeoutException, InterruptedException {
    final OzoneManager[] om = new OzoneManager[1];
    GenericTestUtils.waitFor(() -> {
      om[0] = getOMLeader();
      return om[0] != null;
    }, 200, waitForClusterToBeReadyTimeout);
    return om[0];
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
    for (OzoneManager ozoneManager : omhaService.getServices()) {
      try {
        stopOM(ozoneManager);
      } catch (Exception e) {
        LOG.warn("Failed to stop OM: {}", ozoneManager.getOMServiceId());
      }
    }
    omhaService.inactiveServices().forEachRemaining(omhaService::activate);
    for (OzoneManager ozoneManager : omhaService.getServices()) {
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

    omhaService.inactiveServices().forEachRemaining(om -> {
      if (om.equals(ozoneManager)) {
        this.omhaService.activate(om);
      }
    });
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

  @Override
  public StorageContainerManager getActiveSCM() {
    for (StorageContainerManager scm : scmhaService.getServices()) {
      if (scm.checkLeader()) {
        return scm;
      }
    }
    return null;
  }

  @Override
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
        stopOM(ozoneManager);
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
    stopAndDeactivate(omhaService.getServices().get(index));
  }

  private void stopAndDeactivate(OzoneManager om) {
    stopOM(om);
    omhaService.deactivate(om);
  }

  public void stopOzoneManager(String omNodeId) {
    stopAndDeactivate(omhaService.getServiceById(omNodeId));
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

  private void stopAndDeactivate(StorageContainerManager scm) {
    stopSCM(scm.getSCMNodeId());
    scmhaService.deactivate(scm);
  }

  public void stopSCM(String scmNodeId) {
    stopAndDeactivate(scmhaService.getServiceById(scmNodeId));
  }

  public void deactivateSCM(String scmNodeId) {
    StorageContainerManager scm = scmhaService.getServiceById(scmNodeId);
    scmhaService.deactivate(scm);
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder extends MiniOzoneClusterImpl.Builder {

    private static final String OM_NODE_ID_PREFIX = "omNode-";
    private final List<OzoneManager> activeOMs = new ArrayList<>();
    private final List<OzoneManager> inactiveOMs = new ArrayList<>();

    private static final String SCM_NODE_ID_PREFIX = "scmNode-";
    private final List<StorageContainerManager> activeSCMs = new ArrayList<>();
    private final List<StorageContainerManager> inactiveSCMs = new ArrayList<>();

    private String omServiceId;
    private int numOfOMs;
    private int numOfActiveOMs = ACTIVE_OMS_NOT_SET;

    private String scmServiceId;
    private int numOfSCMs;
    private int numOfActiveSCMs = ACTIVE_SCMS_NOT_SET;

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
    }

    public Builder setNumOfOzoneManagers(int numOMs) {
      this.numOfOMs = numOMs;
      return this;
    }

    public Builder setNumOfActiveOMs(int numActiveOMs) {
      this.numOfActiveOMs = numActiveOMs;
      return this;
    }

    public Builder setOMServiceId(String serviceId) {
      this.omServiceId = serviceId;
      return this;
    }

    public Builder setNumOfStorageContainerManagers(int numSCMs) {
      this.numOfSCMs = numSCMs;
      return this;
    }

    public Builder setNumOfActiveSCMs(int numActiveSCMs) {
      this.numOfActiveSCMs = numActiveSCMs;
      return this;
    }

    public Builder setSCMServiceId(String serviceId) {
      this.scmServiceId = serviceId;
      return this;
    }

    @Override
    public MiniOzoneHAClusterImpl build() throws IOException {
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
      try {
        scmService = createSCMService();
        omService = createOMService();
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes();

      MiniOzoneHAClusterImpl cluster = new MiniOzoneHAClusterImpl(conf,
          scmConfigurator, omService, scmService, hddsDatanodes, path, getServices());
      try {
        cluster.startServices();
      } catch (Exception e) {
        throw new IOException("Unable to start services", e);
      }

      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      prepareForNextBuild();
      return cluster;
    }

    protected int numberOfOzoneManagers() {
      return numOfOMs;
    }

    protected void initOMRatisConf() {
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
        OzoneManager om = createAndStartSingleOM();
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

            OzoneManager.omInit(config);
            OzoneManager om = OzoneManager.createOm(config);
            setClients(om);
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
            stopOM(om);
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
     * Start SCM service with multiple SCMs.
     */
    protected SCMHAService createSCMService()
        throws IOException, AuthenticationException {
      if (scmServiceId == null) {
        StorageContainerManager scm = createAndStartSingleSCM();
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

            configureSCM(true);
            if (i == 1) {
              StorageContainerManager.scmInit(scmConfig, clusterId);
            } else {
              StorageContainerManager.scmBootstrap(scmConfig);
            }
            StorageContainerManager scm =
                HddsTestUtils.getScmSimple(scmConfig, scmConfigurator);
            HealthyPipelineSafeModeRule rule = SafeModeRuleFactory.getInstance()
                .getSafeModeRule(HealthyPipelineSafeModeRule.class);
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
              LOG.info("Initialized SCM at {}. This SCM is currently "
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

      configureScmDatanodeAddress(activeSCMs);

      return new SCMHAService(activeSCMs, inactiveSCMs, scmServiceId);
    }

    /**
     * Initialize HA related configurations.
     */
    private void initSCMHAConfig() {
      // Set configurations required for starting SCM HA service, because that
      // is the serviceID being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_SCM_SERVICE_IDS_KEY, in this
      // way in SCM start it uses internal service id to find it's service id.
      conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
      conf.set(ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID, scmServiceId);
      String scmNodesKey = ConfUtils.addKeySuffixes(
          ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
      StringBuilder scmNodesKeyValue = new StringBuilder();
      StringBuilder scmNames = new StringBuilder();

      for (int i = 1; i <= numOfSCMs; i++) {
        String scmNodeId = SCM_NODE_ID_PREFIX + i;

        if (i == 1) {
          conf.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY, scmNodeId);
        }
        scmNodesKeyValue.append(',').append(scmNodeId);

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
        String scmSecurityAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY, scmServiceId,
            scmNodeId);

        conf.set(scmAddrKey, "127.0.0.1");
        conf.set(scmHttpAddrKey, localhostWithFreePort());
        conf.set(scmHttpsAddrKey, localhostWithFreePort());
        conf.set(scmSecurityAddrKey, localhostWithFreePort());

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
    bootstrapOzoneManager(omNodeId, updateConfigs, force, false);
  }

  public void bootstrapOzoneManager(String omNodeId,
      boolean updateConfigs, boolean force, boolean isListener) throws Exception {

    // Set testReloadConfigFlag to true so that
    // OzoneManager#reloadConfiguration does not reload config as it will
    // return the default configurations.
    OzoneManager.setTestReloadConfigFlag(true);

    int retryCount = 0;
    OzoneManager om = null;

    OzoneManager omLeader = waitForLeaderOM();
    long leaderSnapshotIndex = omLeader.getRatisSnapshotIndex();

    while (true) {
      try {
        OzoneConfiguration newConf = addNewOMToConfig(omhaService.getServiceId(),
            omNodeId, isListener);

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
      String omNodeId, boolean isListener) {
    OzoneConfiguration newConf = new OzoneConfiguration(getConf());
    configureOMPorts(newConf, omServiceId, omNodeId);

    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    newConf.set(omNodesKey, newConf.get(omNodesKey) + "," + omNodeId);

    if (isListener) {
      // append to listener nodes list config
      String listenerOmNodesKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_LISTENER_NODES_KEY, omServiceId);
      String existingListenerNodes = newConf.get(listenerOmNodesKey);
      if (StringUtils.isNotEmpty(existingListenerNodes)) {
        newConf.set(listenerOmNodesKey, existingListenerNodes + "," + omNodeId);
      } else {
        newConf.set(listenerOmNodesKey, omNodeId);
      }
    }
    return newConf;
  }

  /**
   * Update the configurations of the given list of OMs.
   */
  private void updateOMConfigs(OzoneConfiguration newConf) {
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
   * Transfers leadership from current leader to another OM node.
   *
   * @param currentLeader the current leader OM
   * @return the new leader OM after transfer
   */
  public OzoneManager transferOMLeadershipToAnotherNode(OzoneManager currentLeader) throws Exception {
    // Get list of all OMs
    List<OzoneManager> omList = getOzoneManagersList();

    // Remove current leader from list
    omList.remove(currentLeader);

    // Select the first alternative OM as target
    OzoneManager targetOM = omList.get(0);
    String targetNodeId = targetOM.getOMNodeId();

    // Transfer leadership
    currentLeader.transferLeadership(targetNodeId);

    // Wait for leadership transfer to complete
    GenericTestUtils.waitFor(() -> {
      try {
        OzoneManager currentLeaderCheck = getOMLeader();
        return !currentLeaderCheck.getOMNodeId().equals(currentLeader.getOMNodeId());
      } catch (Exception e) {
        return false;
      }
    }, 1000, 30000);

    // Verify leadership change
    waitForLeaderOM();

    return getOMLeader();
  }

  private OzoneConfiguration addNewSCMToConfig(String scmServiceId, String scmNodeId) {
    OzoneConfiguration newConf = new OzoneConfiguration(getConf());
    StringBuilder scmNames = new StringBuilder();
    scmNames.append(newConf.get(ScmConfigKeys.OZONE_SCM_NAMES));
    configureSCMPorts(newConf, scmServiceId, scmNodeId, scmNames);

    String scmNodesKey = ConfUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
    newConf.set(scmNodesKey, newConf.get(scmNodesKey) + "," + scmNodeId);
    newConf.set(ScmConfigKeys.OZONE_SCM_NAMES, scmNames.toString());

    return newConf;
  }

  private static void configureSCMPorts(ConfigurationTarget conf, String scmServiceId, String scmNodeId,
      StringBuilder scmNames) {
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
    String scmSecurityAddrKey = ConfUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY, scmServiceId,
        scmNodeId);

    conf.set(scmAddrKey, "127.0.0.1");
    conf.set(scmHttpAddrKey, localhostWithFreePort());
    conf.set(scmHttpsAddrKey, localhostWithFreePort());
    conf.set(scmSecurityAddrKey, localhostWithFreePort());
    conf.set("ozone.scm.update.service.port", "0");

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

  /**
   * Update the configurations of the given list of SCMs on an SCM HA Service.
   */
  public void updateSCMConfigs(OzoneConfiguration newConf) {
    for (StorageContainerManager scm : scmhaService.getActiveServices()) {
      scm.setConfiguration(newConf);
    }
  }

  public void bootstrapSCM(String scmNodeId, boolean updateConfigs) throws Exception {
    int retryCount = 0;
    StorageContainerManager scm = null;

    StorageContainerManager scmLeader = getScmLeader(true);
    long leaderSnapshotIndex = scmLeader.getScmHAManager().getRatisServer().getSCMStateMachine()
        .getLatestSnapshot().getIndex();

    while (true) {
      try {
        OzoneConfiguration newConf = addNewSCMToConfig(scmhaService.getServiceId(), scmNodeId);

        if (updateConfigs) {
          updateSCMConfigs(newConf);
        }

        scm = bootstrapNewSCM(scmNodeId, newConf);

        LOG.info("Bootstrapped SCM {} RPC server at {}", scmNodeId,
            scm.getClientRpcAddress());

        // Add new SCMs to cluster's in memory map and update existing SCMs conf.
        setConf(newConf);

        break;
      } catch (IOException e) {
        // Existing SCM config could have been updated with new conf. Reset it.
        for (StorageContainerManager existingSCM : scmhaService.getServices()) {
          existingSCM.setConfiguration(getConf());
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

      waitForBootstrappedNodeToBeReady(scm, leaderSnapshotIndex);
      if (updateConfigs) {
        waitForConfigUpdateOnActiveSCMs(scmNodeId);
      }
    }
  }

  /**
   * Start a new SCM in Bootstrap mode. Configs (address and ports) for the new
   * SCM must already be set in the newConf.
   */
  private StorageContainerManager bootstrapNewSCM(String nodeId,
      OzoneConfiguration newConf) throws Exception {
    OzoneConfiguration config = new OzoneConfiguration(newConf);

    // For bootstrapping node, set the nodeId config also.
    config.set(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY, nodeId);

    // Set metadata/DB dir base path
    String metaDirPath = clusterMetaPath + "/" + nodeId;
    config.set(OZONE_METADATA_DIRS, metaDirPath);

    StorageContainerManager.scmBootstrap(config);
    StorageContainerManager scm = StorageContainerManager.createSCM(config);

    scmhaService.addInstance(scm, false);
    startInactiveSCM(nodeId);

    return scm;
  }

  private void waitForBootstrappedNodeToBeReady(StorageContainerManager newSCM,
      long leaderSnapshotIndex) throws Exception {
    // Wait for bootstrapped nodes to catch up with others
    GenericTestUtils.waitFor(() -> {
      return newSCM.getScmHAManager().getRatisServer().getSCMStateMachine()
          .getLatestSnapshot().getIndex() >= leaderSnapshotIndex;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  private void waitForConfigUpdateOnActiveSCMs(
      String newSCMNodeId) throws Exception {
    StorageContainerManager newSCMNode = scmhaService
        .getServiceById(newSCMNodeId);
    GenericTestUtils.waitFor(() -> {
      // Each existing active SCM should contain the new SCM in its peerList.
      // Also, the new SCM should contain each existing active SCM in it's SCM
      // peer list and RatisServer peerList.
      for (StorageContainerManager scm : scmhaService.getActiveServices()) {
        if (!scm.doesPeerExist(scm.getScmId())) {
          return false;
        }
        if (!newSCMNode.doesPeerExist(scm.getSCMNodeId())) {
          return false;
        }
      }
      return true;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  /**
   * MiniOzoneHAService is a helper class used for both SCM and OM HA.
   * This class keeps track of active and inactive OM/SCM services
   * @param <Type>
   */
  static class MiniOzoneHAService<Type> {
    private final Map<String, Type> serviceMap;
    private final List<Type> services;
    private final String serviceId;
    private final String serviceName;

    // Active services s denote OM/SCM services which are up and running
    private final List<Type> activeServices;
    private final List<Type> inactiveServices;

    // Function to extract the Id from service
    private final Function<Type, String> serviceIdProvider;

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
        serviceStarter.accept(service);
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

  @Override
  public StorageContainerManager getStorageContainerManager() {
    return getStorageContainerManagers().get(0);
  }

  private static final class ExitManagerForOM extends ExitManager {

    private final MiniOzoneHAClusterImpl cluster;
    private final String omNodeId;

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
