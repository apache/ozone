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

import java.util.*;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.safemode.HealthyPipelineSafeModeRule;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;

/**
 * MiniOzoneHAClusterImpl creates a complete in-process Ozone cluster
 * with OM HA suitable for running tests.  The cluster consists of a set of
 * OzoneManagers, StorageContainerManager and multiple DataNodes.
 */
public class MiniOzoneHAClusterImpl extends MiniOzoneClusterImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneHAClusterImpl.class);

  private Map<String, OzoneManager> ozoneManagerMap;
  private List<OzoneManager> ozoneManagers;
  private List<StorageContainerManager> storageContainerManagers;
  private String omServiceId;

  // Active OMs denote OMs which are up and running
  private List<OzoneManager> activeOMs;
  private List<OzoneManager> inactiveOMs;

  private int waitForOMToBeReadyTimeout = 120000; // 2 min

  private static final Random RANDOM = new Random();
  private static final int RATIS_LEADER_ELECTION_TIMEOUT = 1000; // 1 second
  public static final int NODE_FAILURE_TIMEOUT = 2000; // 2 seconds

  /**
   * Creates a new MiniOzoneCluster.
   *
   * @throws IOException if there is an I/O error
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private MiniOzoneHAClusterImpl(
      OzoneConfiguration conf,
      List<OzoneManager> activeOMList,
      List<OzoneManager> inactiveOMList,
      List<StorageContainerManager> scms,
      List<HddsDatanodeService> hddsDatanodes,
      String omServiceId,
      ReconServer reconServer) {
    super(conf, hddsDatanodes, reconServer);

    this.ozoneManagerMap = Maps.newHashMap();
    if (activeOMList != null) {
      for (OzoneManager om : activeOMList) {
        this.ozoneManagerMap.put(om.getOMNodeId(), om);
      }
    }
    if (inactiveOMList != null) {
      for (OzoneManager om : inactiveOMList) {
        this.ozoneManagerMap.put(om.getOMNodeId(), om);
      }
    }
    this.ozoneManagers = new ArrayList<>(ozoneManagerMap.values());
    this.activeOMs = activeOMList;
    this.inactiveOMs = inactiveOMList;
    this.storageContainerManagers = scms;
    this.omServiceId = omServiceId;

    // If the serviceID is null, then this should be a non-HA cluster.
    if (omServiceId == null) {
      Preconditions.checkArgument(ozoneManagers.size() <= 1);
    }
  }

  /**
   * Creates a new MiniOzoneCluster with all OMs active.
   * This is used by MiniOzoneChaosCluster.
   */
  protected MiniOzoneHAClusterImpl(
      OzoneConfiguration conf,
      List<OzoneManager> omList,
      StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes,
      String omServiceId) {
    this(conf, omList, null, Arrays.asList(scm), hddsDatanodes, omServiceId, null);
  }

  @Override
  public String getServiceId() {
    return omServiceId;
  }

  /**
   * Returns the first OzoneManager from the list.
   * @return
   */
  @Override
  public OzoneManager getOzoneManager() {
    return this.ozoneManagers.get(0);
  }

  @Override
  public OzoneClient getRpcClient() throws IOException {
    if (omServiceId == null) {
      // Non-HA cluster.
      return OzoneClientFactory.getRpcClient(getConf());
    } else {
      // HA cluster
      return OzoneClientFactory.getRpcClient(omServiceId, getConf());
    }
  }

  public boolean isOMActive(String omNodeId) {
    return activeOMs.contains(ozoneManagerMap.get(omNodeId));
  }

  public OzoneManager getOzoneManager(int index) {
    return this.ozoneManagers.get(index);
  }

  public OzoneManager getOzoneManager(String omNodeId) {
    return this.ozoneManagerMap.get(omNodeId);
  }

  public List<OzoneManager> getOzoneManagersList() {
    return ozoneManagers;
  }

  /**
   * Get OzoneManager leader object.
   * @return OzoneManager object, null if there isn't one or more than one
   */
  public OzoneManager getOMLeader() {
    OzoneManager res = null;
    for (OzoneManager ozoneManager : this.ozoneManagers) {
      if (ozoneManager.isLeader()) {
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

  public StorageContainerManager getSCMLeader() {
    return getSCMLeader(this.storageContainerManagers);
  }

  private static StorageContainerManager getSCMLeader(List<StorageContainerManager> scms) {
    StorageContainerManager res = null;
    for (StorageContainerManager scm : scms) {
      if (scm.getScmHAManager().isLeader()) {
        if (res != null) {
          // Found more than one leader
          // Return null, expect the caller to retry in a while
          return null;
        }
        // Found a leader
        res = scm;
      }
    }
    return res;
  }

  /**
   * Start a previously inactive OM.
   */
  public void startInactiveOM(String omNodeID) throws IOException {
    OzoneManager ozoneManager = ozoneManagerMap.get(omNodeID);
    if (!inactiveOMs.contains(ozoneManager)) {
      throw new IOException("OM is already active.");
    } else {
      ozoneManager.start();
      activeOMs.add(ozoneManager);
      inactiveOMs.remove(ozoneManager);
    }
  }

  @Override
  public void restartOzoneManager() throws IOException {
    for (OzoneManager ozoneManager : ozoneManagers) {
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
          1000, waitForOMToBeReadyTimeout);
    }
  }

  @Override
  public void stop() {
    for (OzoneManager ozoneManager : ozoneManagers) {
      if (ozoneManager != null) {
        LOG.info("Stopping the OzoneManager {}", ozoneManager.getOMNodeId());
        ozoneManager.stop();
        ozoneManager.join();
      }
    }
    super.stop();
  }

  public void stopOzoneManager(int index) {
    ozoneManagers.get(index).stop();
    ozoneManagers.get(index).join();
  }

  public void stopOzoneManager(String omNodeId) {
    ozoneManagerMap.get(omNodeId).stop();
    ozoneManagerMap.get(omNodeId).join();
  }

  /**
   * Builder for configuring the MiniOzoneCluster to run.
   */
  public static class Builder extends MiniOzoneClusterImpl.Builder {

    private final String nodeIdBaseStr = "omNode-";
    private final String scmNodeIdBaseStr = "scmNode-";
    private List<OzoneManager> activeOMs = new ArrayList<>();
    private List<OzoneManager> inactiveOMs = new ArrayList<>();
    private List<StorageContainerManager> scms = new ArrayList<>();

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

      DefaultMetricsSystem.setMiniClusterMode(true);
      initializeConfiguration();
      initOMRatisConf();
      initSCMRatisConf();
      ReconServer reconServer = null;
      try {
        createSCMService();
//        createOMService();
        if (includeRecon) {
          configureRecon();
          reconServer = new ReconServer();
          reconServer.execute(new String[] {});
        }
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

//      StorageContainerManager leaderSCM = getSCMLeader(scms);
//      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(
//              leaderSCM, reconServer);
      final List<HddsDatanodeService> hddsDatanodes = Collections.emptyList();
      MiniOzoneHAClusterImpl cluster = new MiniOzoneHAClusterImpl(conf,
          activeOMs, inactiveOMs, scms, hddsDatanodes, omServiceId, reconServer);

//      if (startDataNodes) {
//        cluster.startHddsDatanodes();
//      }
      return cluster;
    }

    private StorageContainerManager createSCM(OzoneConfiguration conf)
        throws IOException, AuthenticationException {
      SCMStorageConfig scmStore = new SCMStorageConfig(conf);
      initializeScmStorage(scmStore);
      return TestUtils.getScmSimple(conf);
    }

    private void initializeScmStorage(SCMStorageConfig scmStore)
        throws IOException {
      if (scmStore.getState() == StorageState.INITIALIZED) {
        return;
      }
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmServiceId);
      scmStore.initialize();
    }

    protected void initOMRatisConf() {
      conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
      conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numOfOmHandlers);
      // If test change the following config values we will respect,
      // otherwise we will set lower timeout values.
      long defaultDuration =
          OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
              .getDuration();
      long curLeaderElectionTimeout = conf.getTimeDuration(
          OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
              defaultDuration, TimeUnit.MILLISECONDS);
      conf.setTimeDuration(
          OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
          defaultDuration == curLeaderElectionTimeout ?
              RATIS_LEADER_ELECTION_TIMEOUT : curLeaderElectionTimeout,
          TimeUnit.MILLISECONDS);
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

    protected void initSCMRatisConf() {
      conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    }

    protected List<StorageContainerManager> createSCMService()
        throws IOException, AuthenticationException {
      List<StorageContainerManager> scmList = Lists.newArrayList();

      int retryCount = 0;
      int basePort;

      while (true) {
        try {
          basePort = 20000 + RANDOM.nextInt(1000) * 4;
//          initSCMHAConfig(basePort);
          int port = basePort;
          for (int i = 1; i <= numOfSCMs; i++, port+=6) {
            String scmNodeId = scmNodeIdBaseStr + i;
            OzoneConfiguration config = new OzoneConfiguration(conf);
            config.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:" + port);
            config.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:" + (port + 2));
            config.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:" + (port + 3));
            config.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:" + (port + 4));
            config.set(ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, "127.0.0.1:" + (port + 5));
            config.set(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, "127.0.0.1:" + (port + 6));


            String metaDirPath = path + "/" + scmNodeId;
            config.set(OZONE_METADATA_DIRS, metaDirPath);

            scmList.add(createSCM(config));
          }
//          for (int i = 0; i < numOfSCMs; i++) {
//            String nodeId =  scmNodeIdBaseStr + i;
//            OzoneConfiguration config = new OzoneConfiguration(conf);

//            config.set(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY, nodeId);
//            // Set the OM http(s) address to null so that the cluster picks
//            // up the address set with service ID and node ID in initHAConfig
//            config.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "");
//            config.set(ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, "");
//            config.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "");
//            config.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "");
//            config.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "");
//            config.set(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, "");
//
//            String metaDirPath = path + "/" + nodeId;
//            config.set(OZONE_METADATA_DIRS, metaDirPath);
//
//            scmList.add(createSCM(config));
//          }
          for (StorageContainerManager scm : scmList) {
            scm.start();
          }
          break;
        } catch (BindException e) {
          throw new RuntimeException(e);
//          for (StorageContainerManager scm : scmList) {
//            scm.stop();
//            scm.join();
//            LOG.info("Stopping SCM server at client Rpc: {}, Datanode Rpc:{}",
//                scm.getClientRpcAddress(), scm.getDatanodeRpcAddress());
//          }
//          scmList.clear();
//          ++retryCount;
//          LOG.info("MiniOzoneHACluster port conflicts, retried {} times",
//              retryCount);
        }
      }

      scms.clear();
      scms.addAll(scmList);
      return scmList;
    }

    /**
     * Start OM service with multiple OMs.
     */
    protected List<OzoneManager> createOMService() throws IOException,
        AuthenticationException {

      List<OzoneManager> omList = Lists.newArrayList();

      int retryCount = 0;
      int basePort = 10000;

      while (true) {
        try {
          basePort = 10000 + RANDOM.nextInt(1000) * 4;
          initHAConfig(basePort);

          for (int i = 1; i<= numOfOMs; i++) {
            // Set nodeId
            String nodeId = nodeIdBaseStr + i;
            OzoneConfiguration config = new OzoneConfiguration(conf);
            config.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, nodeId);
            // Set the OM http(s) address to null so that the cluster picks
            // up the address set with service ID and node ID in initHAConfig
            config.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "");
            config.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "");

            // Set metadata/DB dir base path
            String metaDirPath = path + "/" + nodeId;
            config.set(OZONE_METADATA_DIRS, metaDirPath);
            OMStorage omStore = new OMStorage(config);
            initializeOmStorage(omStore);

            OzoneManager om = OzoneManager.createOm(config);
            if (certClient != null) {
              om.setCertClient(certClient);
            }
            omList.add(om);

            if (i <= numOfActiveOMs) {
              om.start();
              activeOMs.add(om);
              LOG.info("Started OzoneManager RPC server at {}",
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

    private void initSCMHAConfig(int basePort) throws IOException {
      // Set configurations required for starting SCM HA service.
      conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
      conf.set(ScmConfigKeys.OZONE_SCM_INTERNAL_SERVICE_ID, scmServiceId);
      String scmNodesKey = OmUtils.addKeySuffixes(
          ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
      StringBuilder scmNodesKeyValue = new StringBuilder();
      int port = basePort;
      for (int i = 1; i <= numOfSCMs; i++, port+=6) {
        String scmNodeId = scmNodeIdBaseStr + i;
        scmNodesKeyValue.append(",").append(scmNodeId);
        String scmClientAddrKey = OmUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmBlockClientAddrKey = OmUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmDatanodeAddrKey = OmUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmHttpAddrKey = OmUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmHttpsAddrKey = OmUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, scmServiceId, scmNodeId);
        String scmRatisProtKey = OmUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, scmServiceId, scmNodeId);

        conf.set(scmClientAddrKey, "127.0.0.1:" + port);
        conf.set(scmBlockClientAddrKey, "127.0.0.1:" + (port + 2));
        conf.set(scmDatanodeAddrKey, "127.0.0.1:" + (port + 3));
        conf.setInt(scmHttpAddrKey, port + 4);
        conf.setInt(scmHttpsAddrKey, port + 5);
        conf.setInt(scmRatisProtKey, port + 6);
      }

      conf.set(scmNodesKey, scmNodesKeyValue.substring(1));
    }

    /**
     * Initialize HA related configurations.
     */
    private void initHAConfig(int basePort) throws IOException {
      // Set configurations required for starting OM HA service, because that
      // is the serviceID being passed to start Ozone HA cluster.
      // Here setting internal service and OZONE_OM_SERVICE_IDS_KEY, in this
      // way in OM start it uses internal service id to find it's service id.
      conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, omServiceId);
      conf.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, omServiceId);
      String omNodesKey = OmUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
      StringBuilder omNodesKeyValue = new StringBuilder();

      int port = basePort;

      for (int i = 1; i <= numOfOMs; i++, port+=6) {
        String omNodeId = nodeIdBaseStr + i;
        omNodesKeyValue.append(",").append(omNodeId);
        String omAddrKey = OmUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
        String omHttpAddrKey = OmUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId);
        String omHttpsAddrKey = OmUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId);
        String omRatisPortKey = OmUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, omServiceId, omNodeId);

        conf.set(omAddrKey, "127.0.0.1:" + port);
        conf.set(omHttpAddrKey, "127.0.0.1:" + (port + 2));
        conf.set(omHttpsAddrKey, "127.0.0.1:" + (port + 3));
        conf.setInt(omRatisPortKey, port + 4);
      }

      conf.set(omNodesKey, omNodesKeyValue.substring(1));
    }
  }

  @Override
  public void waitForClusterToBeReady()
          throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      boolean status = true;
      for (StorageContainerManager scm : storageContainerManagers) {
        final int healthy = scm.getNodeCount(HEALTHY);
        final boolean exitSafeMode = !scm.isInSafeMode();
        LOG.info(exitSafeMode ? "Cluster exits safe mode" :
                        "Waiting for cluster to exit safe mode",
                healthy);
        status &= exitSafeMode;
      }

      return status;
    }, 1000, waitForClusterToBeReadyTimeout);
  }
}
