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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.ha.CheckedConsumer;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION;
import static org.apache.hadoop.ozone.om.OmUpgradeConfig.ConfigStrings.OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION;

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

  private static final Random RANDOM = new Random();
  private static final int RATIS_RPC_TIMEOUT = 1000; // 10 second
  public static final int NODE_FAILURE_TIMEOUT = 2000; // 20 seconds

  /**
   * Creates a new MiniOzoneCluster.
   *
   * @throws IOException if there is an I/O error
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public MiniOzoneHAClusterImpl(
      OzoneConfiguration conf,
      List<OzoneManager> activeOMList,
      List<OzoneManager> inactiveOMList,
      List<StorageContainerManager> activeSCMList,
      List<StorageContainerManager> inactiveSCMList,
      List<HddsDatanodeService> hddsDatanodes,
      String omServiceId,
      String scmServiceId,
      String clusterPath,
      ReconServer reconServer) {
    super(conf, hddsDatanodes, reconServer);
    omhaService =
        new OMHAService(activeOMList, inactiveOMList, omServiceId);
    scmhaService =
        new SCMHAService(activeSCMList, inactiveSCMList, scmServiceId);
    this.clusterMetaPath = clusterPath;
  }

  /**
   * Creates a new MiniOzoneCluster with all OMs active.
   * This is used by MiniOzoneChaosCluster.
   */
  protected MiniOzoneHAClusterImpl(
      OzoneConfiguration conf,
      List<OzoneManager> omList,
      List<StorageContainerManager> scmList,
      List<HddsDatanodeService> hddsDatanodes,
      String omServiceId,
      String scmServiceId,
      String clusterPath) {
    this(conf, omList, null, scmList, null, hddsDatanodes,
        omServiceId, scmServiceId, clusterPath, null);
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
  public OzoneClient getRpcClient() throws IOException {
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
    scmhaService.deactivate(scm);
  }

  public void restartStorageContainerManager(
      StorageContainerManager scm, boolean waitForSCM)
      throws IOException, TimeoutException,
      InterruptedException, AuthenticationException {
    LOG.info("Restarting SCM in cluster " + this.getClass());
    OzoneConfiguration scmConf = scm.getConfiguration();
    shutdownStorageContainerManager(scm);
    scm.join();
    scm = TestUtils.getScmSimple(scmConf);
    scmhaService.activate(scm);
    scm.start();
    if (waitForSCM) {
      waitForClusterToBeReady();
    }
  }

  public String getClusterId() throws IOException {
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

    public List<OzoneManager> getActiveOMs() {
      return activeOMs;
    }

    public List<OzoneManager> getInactiveOMs() {
      return inactiveOMs;
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

      // If num of OMs it not set, set it to 1.
      if (numOfSCMs == 0) {
        numOfSCMs = 1;
      }

      // If num of ActiveSCMs is not set, set it to numOfSCMs.
      if (numOfActiveSCMs == ACTIVE_SCMS_NOT_SET) {
        numOfActiveSCMs = numOfSCMs;
      }

      DefaultMetricsSystem.setMiniClusterMode(true);
      initializeConfiguration();
      initOMRatisConf();
      StorageContainerManager scm;
      ReconServer reconServer = null;
      try {
        createSCMService();
        createOMService();
        if (includeRecon) {
          configureRecon();
          reconServer = new ReconServer();
          reconServer.execute(new String[] {});
        }
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(
          activeSCMs, reconServer);

      MiniOzoneHAClusterImpl cluster = new MiniOzoneHAClusterImpl(conf,
          activeOMs, inactiveOMs, activeSCMs, inactiveSCMs,
          hddsDatanodes, omServiceId, scmServiceId, path, reconServer);

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
    protected List<OzoneManager> createOMService() throws IOException,
        AuthenticationException {

      List<OzoneManager> omList = Lists.newArrayList();

      int retryCount = 0;
      int basePort;

      while (true) {
        try {
          basePort = 10000 + RANDOM.nextInt(1000) * 4;
          initOMHAConfig(basePort);

          for (int i = 1; i<= numOfOMs; i++) {
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
              LOG.info("Intialized OzoneManager at {}. This OM is currently "
                  + "inactive (not running).", om.getOmRpcServerAddr());
            }
          }

          // Set default OM address to point to the first OM. Clients would
          // try connecting to this address by default
          conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY,
              NetUtils.getHostPortString(omList.get(0).getOmRpcServerAddr()));

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
              retryCount);
        }
      }
      return omList;
    }

    /**
     * Start OM service with multiple OMs.
     */
    protected List<StorageContainerManager> createSCMService()
        throws IOException, AuthenticationException {
      List<StorageContainerManager> scmList = Lists.newArrayList();

      int retryCount = 0;
      int basePort = 12000;

      while (true) {
        try {
          basePort = 12000 + RANDOM.nextInt(1000) * 4;
          initSCMHAConfig(basePort);

          for (int i = 1; i<= numOfSCMs; i++) {
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
            StorageContainerManager scm = TestUtils.getScmSimple(scmConfig);
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
                  scm.getClientProtocolServer());
            } else {
              inactiveSCMs.add(scm);
              LOG.info("Intialized SCM at {}. This SCM is currently "
                  + "inactive (not running).", scm.getClientProtocolServer());
            }
          }


          break;
        } catch (BindException e) {
          for (StorageContainerManager scm : scmList) {
            scm.stop();
            scm.join();
            LOG.info("Stopping StorageContainerManager server at {}",
                scm.getClientProtocolServer());
          }
          scmList.clear();
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount);
        }
      }
      return scmList;
    }

    /**
     * Initialize HA related configurations.
     */
    private void initSCMHAConfig(int basePort) throws IOException {
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

      int port = basePort;

      for (int i = 1; i <= numOfSCMs; i++, port+=10) {
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
        conf.set(scmHttpAddrKey, "127.0.0.1:" + (port + 2));
        conf.set(scmHttpsAddrKey, "127.0.0.1:" + (port + 3));
        conf.setInt(scmRatisPortKey, port + 4);
        //conf.setInt("ozone.scm.ha.ratis.bind.port", port + 4);
        conf.set(dnPortKey, "127.0.0.1:" + (port + 5));
        conf.set(blockClientKey, "127.0.0.1:" + (port + 6));
        conf.set(ssClientKey, "127.0.0.1:" + (port + 7));
        conf.setInt(scmGrpcPortKey, port + 8);
        scmNames.append(",").append("localhost:" + (port + 5));
        conf.set(ScmConfigKeys.
            OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:" + (port + 6));
      }

      conf.set(scmNodesKey, scmNodesKeyValue.substring(1));
      conf.set(ScmConfigKeys.OZONE_SCM_NAMES, scmNames.substring(1));
    }

    /**
     * Initialize HA related configurations.
     */
    private void initOMHAConfig(int basePort) throws IOException {
      // Set configurations required for starting OM HA service, because that
      // is the serviceID being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_OM_SERVICE_IDS_KEY, in this
      // way in OM start it uses internal service id to find it's service id.
      conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
      conf.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, omServiceId);
      String omNodesKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
      List<String> omNodeIds = new ArrayList<>();

      int port = basePort;

      for (int i = 1; i <= numOfOMs; i++, port+=6) {
        String omNodeId = OM_NODE_ID_PREFIX + i;
        omNodeIds.add(omNodeId);

        String omAddrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
        String omHttpAddrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId);
        String omHttpsAddrKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId);
        String omRatisPortKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

        conf.set(omAddrKey, "127.0.0.1:" + port);
        conf.set(omHttpAddrKey, "127.0.0.1:" + (port + 2));
        conf.set(omHttpsAddrKey, "127.0.0.1:" + (port + 3));
        conf.setInt(omRatisPortKey, port + 4);
      }

      conf.set(omNodesKey, String.join(",", omNodeIds));
    }
  }

  /**
   * Bootstrap new OM and add to existing OM HA service ring.
   * @return new OM nodeId
   */
  public void bootstrapOzoneManager(String omNodeId) throws Exception {

    int basePort;
    int retryCount = 0;

    OzoneManager om = null;

    long leaderSnapshotIndex = getOMLeader().getRatisSnapshotIndex();

    while (true) {
      try {
        basePort = 10000 + RANDOM.nextInt(1000) * 4;
        OzoneConfiguration newConf = addNewOMToConfig(getOMServiceId(),
            omNodeId, basePort);

        om = bootstrapNewOM(omNodeId);

        // Get the CertClient from an existing OM and set for new OM
        if (omhaService.getServiceByIndex(0).getCertificateClient() != null) {
          om.setCertClient(
              omhaService.getServiceByIndex(0).getCertificateClient());
        }

        LOG.info("Bootstrapped OzoneManager {} RPC server at {}", omNodeId,
            om.getOmRpcServerAddr());

        // Add new OMs to cluster's in memory map and update existing OMs conf.
        setConf(newConf);

        omhaService.addInstance(om, true);
        break;
      } catch (IOException e) {
        // Existing OM config could have been updated with new conf. Reset it.
        for (OzoneManager existingOM : omhaService.getServices()) {
          existingOM.setConfiguration(getConf());
        }
        if (e instanceof BindException ||
            e.getCause() instanceof BindException) {
          if (om != null) {
            om.stop();
            om.join();
            LOG.info("Stopping OzoneManager server at {}",
                om.getOmRpcServerAddr());
          }
          ++retryCount;
          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
              retryCount);
        } else {
          throw e;
        }
      }
    }

    waitForBootstrappedNodeToBeReady(om, leaderSnapshotIndex);
    waitForConfigUpdateOnAllOMs(omNodeId);
  }

  /**
   * Set the configs for new OMs.
   */
  private OzoneConfiguration addNewOMToConfig(String omServiceId,
      String omNodeId, int basePort) {
    OzoneConfiguration newConf = getConf();
    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    StringBuilder omNodesKeyValue = new StringBuilder();
    omNodesKeyValue.append(newConf.get(omNodesKey))
        .append(",").append(omNodeId);

    String omAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId);
    String omHttpsAddrKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId);
    String omRatisPortKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

    newConf.set(omAddrKey, "127.0.0.1:" + basePort);
    newConf.set(omHttpAddrKey, "127.0.0.1:" + (basePort + 2));
    newConf.set(omHttpsAddrKey, "127.0.0.1:" + (basePort + 3));
    newConf.setInt(omRatisPortKey, basePort + 4);

    newConf.set(omNodesKey, omNodesKeyValue.toString());

    return newConf;
  }

  /**
   * Start a new OM in Bootstrap mode. Configs for the new OM must already be
   * set.
   */
  private OzoneManager bootstrapNewOM(String nodeId)
      throws IOException, AuthenticationException {
    OzoneConfiguration config = new OzoneConfiguration(getConf());
    config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);
    // Set the OM rpc and http(s) address to null so that the cluster picks
    // up the address set with service ID and node ID
    config.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "");
    config.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "");
    config.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "");

    // Set metadata/DB dir base path
    String metaDirPath = clusterMetaPath + "/" + nodeId;
    config.set(OZONE_METADATA_DIRS, metaDirPath);

    // Update existing OMs config
    for (OzoneManager existingOM : omhaService.getServices()) {
      existingOM.setConfiguration(config);
    }

    OzoneManager.omInit(config);
    OzoneManager om = OzoneManager.createOm(config,
        OzoneManager.StartupOption.BOOTSTRAP);
    om.start();
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

  private void waitForConfigUpdateOnAllOMs(String newOMNodeId)
      throws Exception {
    GenericTestUtils.waitFor(() -> {
      for (OzoneManager om : omhaService.getActiveServices()) {
        if (!om.doesPeerExist(newOMNodeId)) {
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
    private Map<String, Type> serviceMap;
    private List<Type> services;
    private String serviceId;
    private String serviceName;

    // Active services s denote OM/SCM services which are up and running
    private List<Type> activeServices;
    private List<Type> inactiveServices;

    // Function to extract the Id from service
    Function<Type, String> serviceIdProvider;

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
      return services.remove(t);
    }

    public void addInstance(Type t, boolean isActive) {
      services.add(t);
      serviceMap.put(serviceIdProvider.apply(t), t);
      if (isActive) {
        activeServices.add(t);
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
          StorageContainerManager::getScmId);
    }
  }

  public List<StorageContainerManager> getStorageContainerManagers() {
    return this.scmhaService.getServices();
  }

  public StorageContainerManager getStorageContainerManager() {
    return getStorageContainerManagers().get(0);
  }

}