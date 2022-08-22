/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.protobuf.BlockingService;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.OverReplicatedProcessor;
import org.apache.hadoop.hdds.scm.container.replication.UnderReplicatedProcessor;
import org.apache.hadoop.hdds.scm.crl.CRLStatusReportHandler;
import org.apache.hadoop.hdds.scm.ha.BackgroundSCMService;
import org.apache.hadoop.hdds.scm.ha.HASecurityUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.node.NodeAddressUpdateHandler;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManagerImpl;
import org.apache.hadoop.hdds.scm.node.CommandQueueReportHandler;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmHAUnfinalizedStateValidationAction;
import org.apache.hadoop.hdds.scm.pipeline.WritableContainerFactory;
import org.apache.hadoop.hdds.security.token.ContainerTokenGenerator;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.events.EventExecutor;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor;
import org.apache.hadoop.hdds.server.http.RatisDropwizardExports;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.BlockManagerImpl;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerActionsHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.DeadNodeHandler;
import org.apache.hadoop.hdds.scm.node.NewNodeHandler;
import org.apache.hadoop.hdds.scm.node.StartDatanodeAdminHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeReportHandler;
import org.apache.hadoop.hdds.scm.node.HealthyReadOnlyNodeHandler;
import org.apache.hadoop.hdds.scm.node.ReadOnlyHealthyToHealthyNodeHandler;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.StaleNodeHandler;
import org.apache.hadoop.hdds.scm.node.NodeDecommissionManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineActionHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineReportHandler;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.PipelineChoosePolicyFactory;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.security.OzoneSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.MonotonicClock;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore.CertType.VALID_CERTS;
import static org.apache.hadoop.ozone.OzoneConsts.CRL_SEQUENCE_ID_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;

/**
 * StorageContainerManager is the main entry point for the service that
 * provides information about
 * which SCM nodes host containers.
 *
 * <p>DataNodes report to StorageContainerManager using heartbeat messages.
 * SCM allocates containers
 * and returns a pipeline.
 *
 * <p>A client once it gets a pipeline (a list of datanodes) will connect to
 * the datanodes and create a container, which then can be used to store data.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public final class StorageContainerManager extends ServiceRuntimeInfoImpl
    implements SCMMXBean, OzoneStorageContainerManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(StorageContainerManager.class);

  /**
   * SCM metrics.
   */
  private static SCMMetrics metrics;

  /*
   * RPC Endpoints exposed by SCM.
   */
  private final SCMDatanodeProtocolServer datanodeProtocolServer;
  private final SCMBlockProtocolServer blockProtocolServer;
  private final SCMClientProtocolServer clientProtocolServer;
  private SCMSecurityProtocolServer securityProtocolServer;

  /*
   * State Managers of SCM.
   */
  private NodeManager scmNodeManager;
  private PipelineManager pipelineManager;
  private ContainerManager containerManager;
  private BlockManager scmBlockManager;
  private final SCMStorageConfig scmStorageConfig;
  private NodeDecommissionManager scmDecommissionManager;
  private WritableContainerFactory writableContainerFactory;
  private FinalizationManager finalizationManager;
  private HDDSLayoutVersionManager scmLayoutVersionManager;

  private SCMMetadataStore scmMetadataStore;
  private CertificateStore certificateStore;
  private SCMHAManager scmHAManager;
  private SCMContext scmContext;
  private SequenceIdGenerator sequenceIdGen;

  private final EventQueue eventQueue;
  private final SCMServiceManager serviceManager;

  /*
   * HTTP endpoint for JMX access.
   */
  private StorageContainerManagerHttpServer httpServer;
  /**
   * SCM super user.
   */
  private final OzoneAdmins scmAdmins;
  /**
   * SCM mxbean.
   */
  private ObjectName scmInfoBeanName;
  /**
   * Key = DatanodeUuid, value = ContainerStat.
   */
  private final Cache<String, ContainerStat> containerReportCache;

  private ReplicationManager replicationManager;

  private final LeaseManager<Long> commandWatcherLeaseManager;

  private SCMSafeModeManager scmSafeModeManager;
  private SCMCertificateClient scmCertificateClient;
  private ContainerTokenSecretManager containerTokenMgr;

  private JvmPauseMonitor jvmPauseMonitor;
  private final OzoneConfiguration configuration;
  private SCMContainerMetrics scmContainerMetrics;
  private SCMContainerPlacementMetrics placementMetrics;
  private PlacementPolicy containerPlacementPolicy;
  private PlacementPolicy ecContainerPlacementPolicy;
  private PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private MetricsSystem ms;
  private final Map<String, RatisDropwizardExports> ratisMetricsMap =
      new ConcurrentHashMap<>();
  private String primaryScmNodeId;

  /**
   *  Network topology Map.
   */
  private NetworkTopology clusterMap;
  private PipelineChoosePolicy pipelineChoosePolicy;
  private SecurityConfig securityConfig;

  private final SCMHANodeDetails scmHANodeDetails;

  private ContainerBalancer containerBalancer;
  private StatefulServiceStateManager statefulServiceStateManager;
  // Used to keep track of pending replication and pending deletes for
  // container replicas.
  private ContainerReplicaPendingOps containerReplicaPendingOps;

  /**
   * Creates a new StorageContainerManager. Configuration will be
   * updated with information on the actual listening addresses used
   * for RPC servers.
   *
   * @param conf configuration
   */
  @VisibleForTesting
  public StorageContainerManager(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    // default empty configurator means default managers will be used.
    this(conf, new SCMConfigurator());
  }

  /**
   * This constructor offers finer control over how SCM comes up.
   * To use this, user needs to create a SCMConfigurator and set various
   * managers that user wants SCM to use, if a value is missing then SCM will
   * use the default value for that manager.
   *
   * @param conf - Configuration
   * @param configurator - configurator
   */
  @SuppressWarnings("checkstyle:methodlength")
  private StorageContainerManager(OzoneConfiguration conf,
                                  SCMConfigurator configurator)
      throws IOException, AuthenticationException  {
    super(HddsVersionInfo.HDDS_VERSION_INFO);

    Objects.requireNonNull(configurator, "configurator cannot not be null");
    Objects.requireNonNull(conf, "configuration cannot not be null");
    /**
     * It is assumed the scm --init command creates the SCM Storage Config.
     */
    scmStorageConfig = new SCMStorageConfig(conf);

    scmHANodeDetails = SCMHANodeDetails.loadSCMHAConfig(conf, scmStorageConfig);
    configuration = conf;
    initMetrics();
    containerReportCache = buildContainerReportCache();

    if (scmStorageConfig.getState() != StorageState.INITIALIZED) {
      String errMsg = "Please make sure you have run \'ozone scm --init\' " +
          "command to generate all the required metadata to " +
          scmStorageConfig.getStorageDir();
      if (SCMHAUtils.isSCMHAEnabled(conf) && !scmStorageConfig
          .isSCMHAEnabled()) {
        errMsg += " or make sure you have run \'ozone scm --bootstrap\' cmd to "
            + "add the SCM to existing SCM HA group";
      }
      LOG.error(errMsg + ".");
      throw new SCMException("SCM not initialized due to storage config " +
          "failure.", ResultCodes.SCM_NOT_INITIALIZED);
    }

    primaryScmNodeId = scmStorageConfig.getPrimaryScmNodeId();
    initializeCertificateClient();

    /**
     * Important : This initialization sequence is assumed by some of our tests.
     * The testSecureOzoneCluster assumes that security checks have to be
     * passed before any artifacts like SCM DB is created. So please don't
     * add any other initialization above the Security checks please.
     */
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      loginAsSCMUserIfSecurityEnabled(scmHANodeDetails, conf);
    }

    // Creates the SCM DBs or opens them if it exists.
    // A valid pointer to the store is required by all the other services below.
    initalizeMetadataStore(conf, configurator);

    eventQueue = new EventQueue();
    serviceManager = new SCMServiceManager();

    long watcherTimeout =
        conf.getTimeDuration(ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT,
            HDDS_SCM_WATCHER_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    commandWatcherLeaseManager = new LeaseManager<>("CommandWatcher",
        watcherTimeout);
    initializeSystemManagers(conf, configurator);

    // Authenticate SCM if security is enabled, this initialization can only
    // be done after the metadata store is initialized.
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      initializeCAnSecurityProtocol(conf, configurator);
    } else {
      // if no Security, we do not create a Certificate Server at all.
      // This allows user to boot SCM without security temporarily
      // and then come back and enable it without any impact.
      securityProtocolServer = null;
    }

    Collection<String> scmAdminUsernames =
        conf.getTrimmedStringCollection(OzoneConfigKeys.OZONE_ADMINISTRATORS);
    String scmShortUsername =
        UserGroupInformation.getCurrentUser().getShortUserName();

    if (!scmAdminUsernames.contains(scmShortUsername)) {
      scmAdminUsernames.add(scmShortUsername);
    }

    Collection<String> scmAdminGroups =
        conf.getTrimmedStringCollection(
            OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS);

    scmAdmins = new OzoneAdmins(scmAdminUsernames, scmAdminGroups);

    datanodeProtocolServer = new SCMDatanodeProtocolServer(conf, this,
        eventQueue);
    blockProtocolServer = new SCMBlockProtocolServer(conf, this);
    clientProtocolServer = new SCMClientProtocolServer(conf, this);

    initializeEventHandlers();

    containerBalancer = new ContainerBalancer(this);
    LOG.info(containerBalancer.toString());

    // Emit initial safe mode status, as now handlers are registered.
    scmSafeModeManager.emitSafeModeStatus();

    registerMXBean();
    registerMetricsSource(this);
  }


  private void initializeEventHandlers() {
    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(
            pipelineManager, containerManager, scmContext);
    NodeReportHandler nodeReportHandler =
        new NodeReportHandler(scmNodeManager);
    CommandQueueReportHandler commandQueueReportHandler =
        new CommandQueueReportHandler(scmNodeManager);
    PipelineReportHandler pipelineReportHandler =
        new PipelineReportHandler(
            scmSafeModeManager, pipelineManager, scmContext, configuration);
    CommandStatusReportHandler cmdStatusReportHandler =
        new CommandStatusReportHandler();

    NewNodeHandler newNodeHandler = new NewNodeHandler(pipelineManager,
        scmDecommissionManager, configuration, serviceManager);
    NodeAddressUpdateHandler nodeAddressUpdateHandler =
            new NodeAddressUpdateHandler(pipelineManager,
                    scmDecommissionManager, serviceManager);
    StaleNodeHandler staleNodeHandler =
        new StaleNodeHandler(scmNodeManager, pipelineManager, configuration);
    DeadNodeHandler deadNodeHandler = new DeadNodeHandler(scmNodeManager,
        pipelineManager, containerManager);
    StartDatanodeAdminHandler datanodeStartAdminHandler =
        new StartDatanodeAdminHandler(scmNodeManager, pipelineManager);
    ReadOnlyHealthyToHealthyNodeHandler readOnlyHealthyToHealthyNodeHandler =
        new ReadOnlyHealthyToHealthyNodeHandler(configuration, serviceManager);
    HealthyReadOnlyNodeHandler
        healthyReadOnlyNodeHandler =
        new HealthyReadOnlyNodeHandler(scmNodeManager,
            pipelineManager);
    ContainerActionsHandler actionsHandler = new ContainerActionsHandler();

    ContainerReportHandler containerReportHandler =
        new ContainerReportHandler(scmNodeManager, containerManager,
            scmContext, configuration);

    IncrementalContainerReportHandler incrementalContainerReportHandler =
        new IncrementalContainerReportHandler(
            scmNodeManager, containerManager, scmContext);
    PipelineActionHandler pipelineActionHandler =
        new PipelineActionHandler(pipelineManager, scmContext, configuration);
    CRLStatusReportHandler crlStatusReportHandler =
        new CRLStatusReportHandler(certificateStore, configuration);

    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.RETRIABLE_DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.NODE_REPORT, nodeReportHandler);
    eventQueue.addHandler(SCMEvents.COMMAND_QUEUE_REPORT,
        commandQueueReportHandler);

    // Use the same executor for both ICR and FCR.
    // The Executor maps the event to a thread for DN.
    // Dispatcher should always dispatch FCR first followed by ICR
    List<ThreadPoolExecutor> executors =
        FixedThreadPoolWithAffinityExecutor.initializeExecutorPool(
            SCMEvents.CONTAINER_REPORT.getName()
                + "_OR_"
                + SCMEvents.INCREMENTAL_CONTAINER_REPORT.getName());

    EventExecutor<ContainerReportFromDatanode>
        containerReportExecutors =
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(SCMEvents.CONTAINER_REPORT,
                containerReportHandler),
            executors);
    EventExecutor<IncrementalContainerReportFromDatanode>
        incrementalReportExecutors =
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(
                SCMEvents.INCREMENTAL_CONTAINER_REPORT,
                incrementalContainerReportHandler),
            executors);

    eventQueue.addHandler(SCMEvents.CONTAINER_REPORT, containerReportExecutors,
        containerReportHandler);
    eventQueue.addHandler(SCMEvents.INCREMENTAL_CONTAINER_REPORT,
        incrementalReportExecutors,
        incrementalContainerReportHandler);
    eventQueue.addHandler(SCMEvents.CONTAINER_ACTIONS, actionsHandler);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    eventQueue.addHandler(SCMEvents.NEW_NODE, newNodeHandler);
    eventQueue.addHandler(SCMEvents.NODE_ADDRESS_UPDATE,
            nodeAddressUpdateHandler);
    eventQueue.addHandler(SCMEvents.STALE_NODE, staleNodeHandler);
    eventQueue.addHandler(SCMEvents.HEALTHY_READONLY_TO_HEALTHY_NODE,
        readOnlyHealthyToHealthyNodeHandler);
    eventQueue.addHandler(SCMEvents.HEALTHY_READONLY_NODE,
        healthyReadOnlyNodeHandler);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    eventQueue.addHandler(SCMEvents.START_ADMIN_ON_NODE,
        datanodeStartAdminHandler);
    eventQueue.addHandler(SCMEvents.CMD_STATUS_REPORT, cmdStatusReportHandler);
    eventQueue.addHandler(SCMEvents.DELETE_BLOCK_STATUS,
        (DeletedBlockLogImpl) scmBlockManager.getDeletedBlockLog());
    eventQueue.addHandler(SCMEvents.PIPELINE_ACTIONS, pipelineActionHandler);
    eventQueue.addHandler(SCMEvents.PIPELINE_REPORT, pipelineReportHandler);
    eventQueue.addHandler(SCMEvents.CRL_STATUS_REPORT, crlStatusReportHandler);

  }

  private void initializeCertificateClient() {
    securityConfig = new SecurityConfig(configuration);
    if (OzoneSecurityUtil.isSecurityEnabled(configuration) &&
        scmStorageConfig.checkPrimarySCMIdInitialized()) {
      scmCertificateClient = new SCMCertificateClient(
          securityConfig, scmStorageConfig.getScmCertSerialId());
    }
  }

  public OzoneConfiguration getConfiguration() {
    return configuration;
  }

  /**
   * Create an SCM instance based on the supplied configuration.
   *
   * @param conf        HDDS configuration
   * @param configurator SCM configurator
   * @return SCM instance
   * @throws IOException, AuthenticationException
   */
  public static StorageContainerManager createSCM(
      OzoneConfiguration conf, SCMConfigurator configurator)
      throws IOException, AuthenticationException {
    return new StorageContainerManager(conf, configurator);
  }

  /**
   * Create an SCM instance based on the supplied configuration.
   *
   * @param conf        HDDS configuration
   * @return SCM instance
   * @throws IOException, AuthenticationException
   */
  public static StorageContainerManager createSCM(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    return createSCM(conf, new SCMConfigurator());
  }

  /**
   * This function initializes the following managers. If the configurator
   * specifies a value, we will use it, else we will use the default value.
   *
   *  Node Manager
   *  Pipeline Manager
   *  Container Manager
   *  Block Manager
   *  Replication Manager
   *  Safe Mode Manager
   *
   * @param conf - Ozone Configuration.
   * @param configurator - A customizer which allows different managers to be
   *                    used if needed.
   * @throws IOException - on Failure.
   */
  @SuppressWarnings("methodLength")
  private void initializeSystemManagers(OzoneConfiguration conf,
      SCMConfigurator configurator) throws IOException {
    Clock clock = new MonotonicClock(ZoneOffset.UTC);

    if (configurator.getNetworkTopology() != null) {
      clusterMap = configurator.getNetworkTopology();
    } else {
      clusterMap = new NetworkTopologyImpl(conf);
    }
    // This needs to be done before initializing Ratis.
    RatisDropwizardExports.registerRatisMetricReporters(ratisMetricsMap);
    if (configurator.getSCMHAManager() != null) {
      scmHAManager = configurator.getSCMHAManager();
    } else {
      scmHAManager = new SCMHAManagerImpl(conf, this);
    }

    scmLayoutVersionManager = new HDDSLayoutVersionManager(
        scmStorageConfig.getLayoutVersion());

    UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext>
        finalizationExecutor;
    if (configurator.getUpgradeFinalizationExecutor() != null) {
      finalizationExecutor = configurator.getUpgradeFinalizationExecutor();
    } else {
      finalizationExecutor = new DefaultUpgradeFinalizationExecutor<>();
    }
    finalizationManager = new FinalizationManagerImpl.Builder()
        .setConfiguration(conf)
        .setLayoutVersionManager(scmLayoutVersionManager)
        .setStorage(scmStorageConfig)
        .setHAManager(scmHAManager)
        .setFinalizationStore(scmMetadataStore.getMetaTable())
        .setFinalizationExecutor(finalizationExecutor)
        .build();

    // inline upgrade for SequenceIdGenerator
    SequenceIdGenerator.upgradeToSequenceId(scmMetadataStore);
    // Distributed sequence id generator
    sequenceIdGen = new SequenceIdGenerator(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable());

    if (configurator.getScmContext() != null) {
      scmContext = configurator.getScmContext();
    } else {
      // When term equals SCMContext.INVALID_TERM, the isLeader() check
      // and getTermOfLeader() will always pass.
      long term = SCMHAUtils.isSCMHAEnabled(conf) ? 0 : SCMContext.INVALID_TERM;
      // non-leader of term 0, in safe mode, preCheck not completed.
      scmContext = new SCMContext.Builder()
          .setLeader(false)
          .setTerm(term)
          .setIsInSafeMode(true)
          .setIsPreCheckComplete(false)
          .setSCM(this)
          .setFinalizationCheckpoint(finalizationManager.getCheckpoint())
          .build();
    }

    if (configurator.getScmNodeManager() != null) {
      scmNodeManager = configurator.getScmNodeManager();
    } else {
      scmNodeManager = new SCMNodeManager(conf, scmStorageConfig, eventQueue,
          clusterMap, scmContext, scmLayoutVersionManager);
    }

    placementMetrics = SCMContainerPlacementMetrics.create();
    containerPlacementPolicy =
        ContainerPlacementPolicyFactory.getPolicy(conf, scmNodeManager,
            clusterMap, true, placementMetrics);

    ecContainerPlacementPolicy = ContainerPlacementPolicyFactory.getECPolicy(
        conf, scmNodeManager, clusterMap, true, placementMetrics);

    placementPolicyValidateProxy = new PlacementPolicyValidateProxy(
        containerPlacementPolicy, ecContainerPlacementPolicy);

    if (configurator.getPipelineManager() != null) {
      pipelineManager = configurator.getPipelineManager();
    } else {
      pipelineManager =
          PipelineManagerImpl.newPipelineManager(
              conf,
              scmHAManager,
              scmNodeManager,
              scmMetadataStore.getPipelineTable(),
              eventQueue,
              scmContext,
              serviceManager,
              clock
              );
    }

    finalizationManager.buildUpgradeContext(scmNodeManager, pipelineManager,
        scmContext);

    containerReplicaPendingOps = new ContainerReplicaPendingOps(conf, clock);

    long containerReplicaOpScrubberIntervalMs = conf.getTimeDuration(
        ScmConfigKeys
            .OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        ScmConfigKeys
            .OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    long containerReplicaOpExpiryMs = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_CONTAINER_REPLICA_OP_TIME_OUT,
        ScmConfigKeys.OZONE_SCM_CONTAINER_REPLICA_OP_TIME_OUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    long backgroundServiceSafemodeWaitMs = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);

    final String backgroundServiceName = "ExpiredContainerReplicaOpScrubber";
    BackgroundSCMService expiredContainerReplicaOpScrubber =
        new BackgroundSCMService.Builder().setClock(clock)
            .setScmContext(scmContext)
            .setServiceName(backgroundServiceName)
            .setIntervalInMillis(containerReplicaOpScrubberIntervalMs)
            .setWaitTimeInMillis(backgroundServiceSafemodeWaitMs)
            .setPeriodicalTask(() -> containerReplicaPendingOps
                .removeExpiredEntries(containerReplicaOpExpiryMs)).build();

    serviceManager.register(expiredContainerReplicaOpScrubber);

    if (configurator.getContainerManager() != null) {
      containerManager = configurator.getContainerManager();
    } else {
      containerManager = new ContainerManagerImpl(conf, scmHAManager,
          sequenceIdGen, pipelineManager, scmMetadataStore.getContainerTable(),
          containerReplicaPendingOps);
    }

    pipelineChoosePolicy = PipelineChoosePolicyFactory.getPolicy(conf);
    if (configurator.getWritableContainerFactory() != null) {
      writableContainerFactory = configurator.getWritableContainerFactory();
    } else {
      writableContainerFactory = new WritableContainerFactory(this);
    }
    if (configurator.getScmBlockManager() != null) {
      scmBlockManager = configurator.getScmBlockManager();
    } else {
      scmBlockManager = new BlockManagerImpl(conf, this);
    }
    if (configurator.getReplicationManager() != null) {
      replicationManager = configurator.getReplicationManager();
    }  else {
      LegacyReplicationManager legacyRM = new LegacyReplicationManager(
          conf, containerManager, containerPlacementPolicy, eventQueue,
          scmContext, scmNodeManager, scmHAManager, clock,
          getScmMetadataStore().getMoveTable());
      replicationManager = new ReplicationManager(
          conf,
          containerManager,
          containerPlacementPolicy,
          eventQueue,
          scmContext,
          scmNodeManager,
          clock,
          legacyRM,
          containerReplicaPendingOps);
      ReplicationManager.ReplicationManagerConfiguration rmConf = conf
          .getObject(ReplicationManager.ReplicationManagerConfiguration.class);

      UnderReplicatedProcessor underReplicatedProcessor =
          new UnderReplicatedProcessor(replicationManager,
              containerReplicaPendingOps, eventQueue);

      BackgroundSCMService underReplicatedQueueThread =
          new BackgroundSCMService.Builder().setClock(clock)
              .setScmContext(scmContext)
              .setServiceName("UnderReplicatedQueueThread")
              .setIntervalInMillis(rmConf.getUnderReplicatedInterval())
              .setWaitTimeInMillis(backgroundServiceSafemodeWaitMs)
              .setPeriodicalTask(underReplicatedProcessor::processAll).build();
      serviceManager.register(underReplicatedQueueThread);

      OverReplicatedProcessor overReplicatedProcessor =
          new OverReplicatedProcessor(replicationManager,
              containerReplicaPendingOps, eventQueue);

      BackgroundSCMService overReplicatedQueueThread =
          new BackgroundSCMService.Builder().setClock(clock)
              .setScmContext(scmContext)
              .setServiceName("OverReplicatedQueueThread")
              .setIntervalInMillis(rmConf.getOverReplicatedInterval())
              .setWaitTimeInMillis(backgroundServiceSafemodeWaitMs)
              .setPeriodicalTask(overReplicatedProcessor::processAll).build();
      serviceManager.register(overReplicatedQueueThread);
    }
    serviceManager.register(replicationManager);
    if (configurator.getScmSafeModeManager() != null) {
      scmSafeModeManager = configurator.getScmSafeModeManager();
    } else {
      scmSafeModeManager = new SCMSafeModeManager(conf,
          containerManager.getContainers(), containerManager,
          pipelineManager, eventQueue, serviceManager, scmContext);
    }

    scmDecommissionManager = new NodeDecommissionManager(conf, scmNodeManager,
        containerManager, scmContext, eventQueue, replicationManager);

    statefulServiceStateManager = StatefulServiceStateManagerImpl.newBuilder()
        .setStatefulServiceConfig(
            scmMetadataStore.getStatefulServiceConfigTable())
        .setSCMDBTransactionBuffer(scmHAManager.getDBTransactionBuffer())
        .setRatisServer(scmHAManager.getRatisServer())
        .build();
  }

  /**
   * If security is enabled we need to have the Security Protocol and a
   * default CA. This function initializes those values based on the
   * configurator.
   *
   * @param conf - Config
   * @param configurator - configurator
   * @throws IOException - on Failure
   * @throws AuthenticationException - on Failure
   */
  private void initializeCAnSecurityProtocol(OzoneConfiguration conf,
      SCMConfigurator configurator) throws IOException {

    // TODO: Support Certificate Server loading via Class Name loader.
    // So it is easy to use different Certificate Servers if needed.
    if (this.scmMetadataStore == null) {
      LOG.error("Cannot initialize Certificate Server without a valid meta " +
          "data layer.");
      throw new SCMException("Cannot initialize CA without a valid metadata " +
          "store", ResultCodes.SCM_NOT_INITIALIZED);
    }

    certificateStore =
        new SCMCertStore.Builder().setMetadaStore(scmMetadataStore)
            .setRatisServer(scmHAManager.getRatisServer())
            .setCRLSequenceId(getLastSequenceIdForCRL()).build();


    final CertificateServer scmCertificateServer;
    final CertificateServer rootCertificateServer;
    // If primary SCM node Id is set it means this is a cluster which has
    // performed init with SCM HA version code.
    if (scmStorageConfig.checkPrimarySCMIdInitialized()) {
      // Start specific instance SCM CA server.
      String subject = SCM_SUB_CA_PREFIX +
          InetAddress.getLocalHost().getHostName();
      if (configurator.getCertificateServer() != null) {
        scmCertificateServer = configurator.getCertificateServer();
      } else {
        scmCertificateServer = new DefaultCAServer(subject,
            scmStorageConfig.getClusterID(), scmStorageConfig.getScmId(),
            certificateStore, new DefaultProfile(),
            scmCertificateClient.getComponentName());
        // INTERMEDIARY_CA which issues certs to DN and OM.
        scmCertificateServer.init(new SecurityConfig(configuration),
            CertificateServer.CAType.INTERMEDIARY_CA);
      }

      if (primaryScmNodeId.equals(scmStorageConfig.getScmId())) {
        if (configurator.getCertificateServer() != null) {
          rootCertificateServer = configurator.getCertificateServer();
        } else {
          rootCertificateServer =
              HASecurityUtils.initializeRootCertificateServer(
              conf, certificateStore, scmStorageConfig, new DefaultCAProfile());
        }
        persistPrimarySCMCerts();
      } else {
        rootCertificateServer = null;
      }
    } else {
      // On a upgraded cluster primary scm nodeId will not be set as init will
      // not be run again after upgrade. So for a upgraded cluster where init
      // has not happened again we will have setup like before where it has
      // one CA server which is issuing certificates to DN and OM.
      rootCertificateServer =
          HASecurityUtils.initializeRootCertificateServer(conf,
              certificateStore, scmStorageConfig, new DefaultProfile());
      scmCertificateServer = rootCertificateServer;
    }

    // We need to pass getCACertificate as rootCA certificate,
    // as for SCM CA is root-CA.
    securityProtocolServer = new SCMSecurityProtocolServer(conf,
        rootCertificateServer, scmCertificateServer,
        scmCertificateClient != null ?
            scmCertificateClient.getCACertificate() : null, this);

    if (securityConfig.isContainerTokenEnabled()) {
      containerTokenMgr = createContainerTokenSecretManager(configuration);
    }
  }

  /** Persist primary SCM root ca cert and sub-ca certs to DB.
   *
   * @throws IOException
   */
  private void persistPrimarySCMCerts() throws IOException {
    BigInteger certSerial =
        scmCertificateClient.getCertificate().getSerialNumber();
    // Store the certificate in DB. On primary SCM when init happens, the
    // certificate is not persisted to DB. As we don't have Metadatstore
    // and ratis server initialized with statemachine. We need to do only
    // for primary scm, for other bootstrapped scm's certificates will be
    // persisted via ratis.
    if (certificateStore.getCertificateByID(certSerial,
        VALID_CERTS) == null) {
      LOG.info("Storing sub-ca certificate serialId {} on primary SCM",
          certSerial);
      certificateStore.storeValidScmCertificate(
          certSerial, scmCertificateClient.getCertificate());
    }
    X509Certificate rootCACert = scmCertificateClient.getCACertificate();
    if (certificateStore.getCertificateByID(rootCACert.getSerialNumber(),
        VALID_CERTS) == null) {
      LOG.info("Storing root certificate serialId {}",
          rootCACert.getSerialNumber());
      certificateStore.storeValidScmCertificate(
          rootCACert.getSerialNumber(), rootCACert);
    }
  }

  public CertificateServer getRootCertificateServer() {
    return getSecurityProtocolServer().getRootCertificateServer();
  }

  public CertificateServer getScmCertificateServer() {
    return getSecurityProtocolServer().getScmCertificateServer();
  }

  public SCMCertificateClient getScmCertificateClient() {
    return scmCertificateClient;
  }

  private ContainerTokenSecretManager createContainerTokenSecretManager(
      OzoneConfiguration conf) throws IOException {

    long expiryTime = conf.getTimeDuration(
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME,
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);

    // Means this is an upgraded cluster and it has no sub-ca,
    // so SCM Certificate client is not initialized. To make Tokens
    // work let's use root CA cert and create SCM Certificate client with
    // root CA cert.
    if (scmCertificateClient == null) {
      Preconditions.checkState(
          !scmStorageConfig.checkPrimarySCMIdInitialized());

      String certSerialNumber;
      try {
        certSerialNumber = getScmCertificateServer().getCACertificate()
            .getSerialNumber().toString();
      } catch (CertificateException ex) {
        LOG.error("Get CA Certificate failed", ex);
        throw new IOException(ex);
      } catch (IOException ex) {
        LOG.error("Get CA Certificate failed", ex);
        throw ex;
      }
      scmCertificateClient = new SCMCertificateClient(securityConfig,
          certSerialNumber, SCM_ROOT_CA_COMPONENT_NAME);
    }
    String certId = scmCertificateClient.getCertificate().getSerialNumber()
        .toString();
    return new ContainerTokenSecretManager(securityConfig,
        expiryTime, certId);
  }

  /**
   * Init the metadata store based on the configurator.
   * @param conf - Config
   * @param configurator - configurator
   * @throws IOException - on Failure
   */
  private void initalizeMetadataStore(OzoneConfiguration conf,
                                      SCMConfigurator configurator)
      throws IOException {
    if (configurator.getMetadataStore() != null) {
      scmMetadataStore = configurator.getMetadataStore();
    } else {
      scmMetadataStore = new SCMMetadataStoreImpl(conf);
    }
  }

  /**
   * Login as the configured user for SCM.
   *
   * @param conf
   */
  private static void loginAsSCMUserIfSecurityEnabled(
      SCMHANodeDetails scmhaNodeDetails, ConfigurationSource conf)
      throws IOException, AuthenticationException {
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      if (LOG.isDebugEnabled()) {
        ScmConfig scmConfig = conf.getObject(ScmConfig.class);
        LOG.debug("Ozone security is enabled. Attempting login for SCM user. "
                + "Principal: {}, keytab: {}",
            scmConfig.getKerberosPrincipal(),
            scmConfig.getKerberosKeytab());
      }

      Configuration hadoopConf =
          LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
      if (SecurityUtil.getAuthenticationMethod(hadoopConf).equals(
          AuthenticationMethod.KERBEROS)) {
        UserGroupInformation.setConfiguration(hadoopConf);
        InetSocketAddress socketAddress = getScmAddress(scmhaNodeDetails, conf);
        SecurityUtil.login(hadoopConf,
            ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
            ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
            socketAddress.getHostName());
      } else {
        throw new AuthenticationException(SecurityUtil.getAuthenticationMethod(
            hadoopConf) + " authentication method not support. "
            + "SCM user login failed.");
      }
      LOG.info("SCM login successful.");
    }
  }

  long getLastSequenceIdForCRL() throws IOException {
    Long sequenceId =
        scmMetadataStore.getCRLSequenceIdTable().get(CRL_SEQUENCE_ID_KEY);
    // If the CRL_SEQUENCE_ID_KEY does not exist in DB return 0 so that new
    // CRL requests can have sequence id starting from 1.
    if (sequenceId == null) {
      return 0L;
    }
    // If there exists a last sequence id in the DB, the new incoming
    // CRL requests must have sequence ids greater than the one stored in the DB
    return sequenceId;
  }

  /**
   * Builds a message for logging startup information about an RPC server.
   *
   * @param description RPC server description
   * @param addr        RPC server listening address
   * @return server startup message
   */
  public static String buildRpcServerStartMessage(String description,
                                                  InetSocketAddress addr) {
    return addr != null
        ? String.format("%s is listening at %s", description, addr.toString())
        : String.format("%s not started", description);
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  public static RPC.Server startRpcServer(
      OzoneConfiguration conf,
      InetSocketAddress addr,
      Class<?> protocol,
      BlockingService instance,
      int handlerCount)
      throws IOException {
    RPC.Server rpcServer =
        new RPC.Builder(conf)
            .setProtocol(protocol)
            .setInstance(instance)
            .setBindAddress(addr.getHostString())
            .setPort(addr.getPort())
            .setNumHandlers(handlerCount)
            .setVerbose(false)
            .setSecretManager(null)
            .build();

    HddsServerUtil.addPBProtocol(conf, protocol, instance, rpcServer);
    return rpcServer;
  }

  /**
   * Routine to bootstrap the StorageContainerManager. This will connect to a
   * running SCM instance which has valid cluster id and fetch the cluster id
   * from there.
   *
   * TODO: once SCM HA security is enabled, CSR cerificates will be fetched from
   * running scm leader instance as well.
   *
   * @param conf OzoneConfiguration
   * @return true if SCM bootstrap is successful, false otherwise.
   * @throws IOException if init fails due to I/O error
   */
  public static boolean scmBootstrap(OzoneConfiguration conf)
      throws AuthenticationException, IOException {
    if (!SCMHAUtils.isSCMHAEnabled(conf)) {
      LOG.error("Bootstrap is not supported without SCM HA.");
      return false;
    }
    String primordialSCM = SCMHAUtils.getPrimordialSCM(conf);
    SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    SCMHANodeDetails scmhaNodeDetails = SCMHANodeDetails.loadSCMHAConfig(conf,
        scmStorageConfig);
    String selfNodeId = scmhaNodeDetails.getLocalNodeDetails().getNodeId();
    final String selfHostName =
        scmhaNodeDetails.getLocalNodeDetails().getHostName();
    if (primordialSCM != null && SCMHAUtils.isSCMHAEnabled(conf)
        && SCMHAUtils.isPrimordialSCM(conf, selfNodeId, selfHostName)) {
      LOG.info(
          "SCM bootstrap command can only be executed in non-Primordial SCM "
              + "{}, self id {} " + "Ignoring it.", primordialSCM, selfNodeId);
      return true;
    }
    final String persistedClusterId = scmStorageConfig.getClusterID();
    StorageState state = scmStorageConfig.getState();
    if (state == StorageState.INITIALIZED && conf
        .getBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
            ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_DEFAULT)) {
      LOG.info("Skipping clusterId validation during bootstrap command.  "
              + "ClusterId id {}, SCM id {}", persistedClusterId,
          scmStorageConfig.getScmId());

      // Initialize security if security is enabled later.
      initializeSecurityIfNeeded(conf, scmhaNodeDetails, scmStorageConfig);

      return true;
    }

    loginAsSCMUserIfSecurityEnabled(scmhaNodeDetails, conf);
    // The node here will try to fetch the cluster id from any of existing
    // running SCM instances.
    OzoneConfiguration config =
        SCMHAUtils.removeSelfId(conf,
            scmhaNodeDetails.getLocalNodeDetails().getNodeId());
    final ScmInfo scmInfo = HAUtils.getScmInfo(config);
    final String fetchedId = scmInfo.getClusterId();
    Preconditions.checkNotNull(fetchedId);
    if (state == StorageState.INITIALIZED) {
      Preconditions.checkNotNull(scmStorageConfig.getScmId());
      if (!fetchedId.equals(persistedClusterId)) {
        LOG.error(
            "Could not bootstrap as SCM is already initialized with cluster "
                + "id {} but cluster id for existing leader SCM instance "
                + "is {}", persistedClusterId, fetchedId);
        return false;
      }

      // Initialize security if security is enabled later.
      initializeSecurityIfNeeded(conf, scmhaNodeDetails, scmStorageConfig);

    } else {
      try {
        scmStorageConfig.setClusterId(fetchedId);
        // It will write down the cluster Id fetched from already
        // running SCM as well as the local SCM Id.

        // SCM Node info containing hostname to scm Id mappings
        // will be persisted into the version file once this node gets added
        // to existing SCM ring post node regular start up.

        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          HASecurityUtils.initializeSecurity(scmStorageConfig, config,
              getScmAddress(scmhaNodeDetails, conf), false);
        }
        scmStorageConfig.setPrimaryScmNodeId(scmInfo.getScmId());
        scmStorageConfig.setSCMHAFlag(true);
        scmStorageConfig.initialize();
        LOG.info("SCM BootStrap  is successful for ClusterID {}, SCMID {}",
            scmInfo.getClusterId(), scmStorageConfig.getScmId());
        LOG.info("Primary SCM Node ID {}",
            scmStorageConfig.getPrimaryScmNodeId());
      } catch (IOException ioe) {
        LOG.error("Could not initialize SCM version file", ioe);
        return false;
      }
    }
    return true;
  }

  /**
   * Initialize security If Ozone security is enabled and
   * ScmStorageConfig does not have certificate serial id.
   * @param conf
   * @param scmhaNodeDetails
   * @param scmStorageConfig
   * @throws IOException
   */
  private static void initializeSecurityIfNeeded(OzoneConfiguration conf,
      SCMHANodeDetails scmhaNodeDetails, SCMStorageConfig scmStorageConfig)
      throws IOException {
    // Initialize security if security is enabled later.
    if (OzoneSecurityUtil.isSecurityEnabled(conf)
        && scmStorageConfig.getScmCertSerialId() == null) {
      HASecurityUtils.initializeSecurity(scmStorageConfig, conf,
          getScmAddress(scmhaNodeDetails, conf), true);
      scmStorageConfig.forceInitialize();
      LOG.info("SCM unsecure cluster is converted to secure cluster. " +
              "Persisted SCM Certificate SerialID {}",
          scmStorageConfig.getScmCertSerialId());
    }
  }

  /**
   * Routine to set up the Version info for StorageContainerManager.
   *
   * @param conf OzoneConfiguration
   * @return true if SCM initialization is successful, false otherwise.
   * @throws IOException if init fails due to I/O error
   */
  public static boolean scmInit(OzoneConfiguration conf,
      String clusterId) throws IOException {
    SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    StorageState state = scmStorageConfig.getState();
    final SCMHANodeDetails haDetails = SCMHANodeDetails.loadSCMHAConfig(conf,
        scmStorageConfig);
    String primordialSCM = SCMHAUtils.getPrimordialSCM(conf);
    final String selfNodeId = haDetails.getLocalNodeDetails().getNodeId();
    final String selfHostName = haDetails.getLocalNodeDetails().getHostName();
    if (primordialSCM != null && SCMHAUtils.isSCMHAEnabled(conf)
        && !SCMHAUtils.isPrimordialSCM(conf, selfNodeId, selfHostName)) {
      LOG.info(
          "SCM init command can only be executed in Primordial SCM {}, "
              + "self id {} "
              + "Ignoring it.", primordialSCM, selfNodeId);
      return true;
    }
    if (state != StorageState.INITIALIZED) {
      try {
        if (clusterId != null && !clusterId.isEmpty()) {
          // clusterId must be an UUID
          Preconditions.checkNotNull(UUID.fromString(clusterId));
          scmStorageConfig.setClusterId(clusterId);
        }


        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          HASecurityUtils.initializeSecurity(scmStorageConfig, conf,
              getScmAddress(haDetails, conf), true);
        }

        // Ensure scmRatisServer#initialize() is called post scm storage
        // config initialization.. If SCM version file is created,
        // the subsequent scm init should use the clusterID from version file.
        // So, scmStorageConfig#initialize() should happen before ratis server
        // initialize. In this way,we do not leave ratis storage directory
        // with multiple raft group directories in failure scenario.

        // The order of init should be
        // 1. SCM storage config initialize to create version file.
        // 2. Initialize Ratis server.

        scmStorageConfig.setPrimaryScmNodeId(scmStorageConfig.getScmId());
        scmStorageConfig.initialize();

        if (SCMHAUtils.isSCMHAEnabled(conf)) {
          SCMRatisServerImpl.initialize(scmStorageConfig.getClusterID(),
              scmStorageConfig.getScmId(), haDetails.getLocalNodeDetails(),
              conf);
          scmStorageConfig = new SCMStorageConfig(conf);
          scmStorageConfig.setSCMHAFlag(true);
          // Do force initialize to persist SCM_HA flag.
          scmStorageConfig.forceInitialize();
        }

        LOG.info("SCM initialization succeeded. Current cluster id for sd={}"
                + "; cid={}; layoutVersion={}; scmId={}",
            scmStorageConfig.getStorageDir(), scmStorageConfig.getClusterID(),
            scmStorageConfig.getLayoutVersion(), scmStorageConfig.getScmId());
        return true;
      } catch (IOException ioe) {
        LOG.error("Could not initialize SCM version file", ioe);
        return false;
      }
    } else {
      // If SCM HA was not being used before pre-finalize, and is being used
      // when the cluster is pre-finalized for the SCM HA feature, init
      // should fail.
      ScmHAUnfinalizedStateValidationAction.checkScmHA(conf, scmStorageConfig,
          new HDDSLayoutVersionManager(scmStorageConfig.getLayoutVersion()));

      clusterId = scmStorageConfig.getClusterID();
      final boolean isSCMHAEnabled = scmStorageConfig.isSCMHAEnabled();

      // Initialize security if security is enabled later.
      initializeSecurityIfNeeded(conf, haDetails, scmStorageConfig);

      if (SCMHAUtils.isSCMHAEnabled(conf) && !isSCMHAEnabled) {
        SCMRatisServerImpl.initialize(scmStorageConfig.getClusterID(),
            scmStorageConfig.getScmId(), haDetails.getLocalNodeDetails(),
            conf);
        scmStorageConfig.setSCMHAFlag(true);
        scmStorageConfig.setPrimaryScmNodeId(scmStorageConfig.getScmId());
        scmStorageConfig.forceInitialize();
        LOG.debug("Enabled SCM HA");
      }

      LOG.info("SCM already initialized. Reusing existing cluster id for sd={}"
              + ";cid={}; layoutVersion={}; HAEnabled={}",
          scmStorageConfig.getStorageDir(), clusterId,
          scmStorageConfig.getLayoutVersion(),
          scmStorageConfig.isSCMHAEnabled());
      return true;
    }
  }

  private static InetSocketAddress getScmAddress(SCMHANodeDetails haDetails,
      ConfigurationSource conf) throws IOException {
    List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(
        conf);
    Preconditions.checkNotNull(scmNodeInfoList, "scmNodeInfoList is null");

    InetSocketAddress scmAddress = null;
    if (SCMHAUtils.getScmServiceId(conf) != null) {
      for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
        if (haDetails.getLocalNodeDetails().getNodeId() != null
            && haDetails.getLocalNodeDetails().getNodeId().equals(
            scmNodeInfo.getNodeId())) {
          scmAddress =
              NetUtils.createSocketAddr(scmNodeInfo.getBlockClientAddress());
        }
      }
    } else  {
      // Get Local host and use scm client port
      if (scmNodeInfoList.get(0).getBlockClientAddress() == null) {
        LOG.error("SCM Address not able to figure out from config, finding " +
            "hostname from InetAddress.");
        scmAddress =
            NetUtils.createSocketAddr(InetAddress.getLocalHost().getHostName(),
                ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT);
      } else {
        scmAddress = NetUtils.createSocketAddr(
            scmNodeInfoList.get(0).getBlockClientAddress());
      }
    }


    return scmAddress;
  }

  /**
   * Initialize SCM metrics.
   */
  public static void initMetrics() {
    metrics = SCMMetrics.create();
  }

  /**
   * Return SCM metrics instance.
   */
  public static SCMMetrics getMetrics() {
    return metrics == null ? SCMMetrics.create() : metrics;
  }

  public SCMStorageConfig getScmStorageConfig() {
    return scmStorageConfig;
  }

  public SCMDatanodeProtocolServer getDatanodeProtocolServer() {
    return datanodeProtocolServer;
  }

  public SCMBlockProtocolServer getBlockProtocolServer() {
    return blockProtocolServer;
  }

  public SCMClientProtocolServer getClientProtocolServer() {
    return clientProtocolServer;
  }

  public SCMSecurityProtocolServer getSecurityProtocolServer() {
    return securityProtocolServer;
  }

  /**
   * Initialize container reports cache that sent from datanodes.
   */
  @SuppressWarnings("UnstableApiUsage")
  private Cache<String, ContainerStat> buildContainerReportCache() {
    return
        CacheBuilder.newBuilder()
            .expireAfterAccess(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            .maximumSize(Integer.MAX_VALUE)
            .removalListener((
                RemovalListener<String, ContainerStat>) removalNotification -> {
                synchronized (containerReportCache) {
                  ContainerStat stat = removalNotification.getValue();
                  if (stat != null) {
                    // TODO: Are we doing the right thing here?
                    // remove invalid container report
                    metrics.decrContainerStat(stat);
                  }
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Remove expired container stat entry for " +
                        "datanode: {}.", removalNotification.getKey());
                  }
                }
              })
            .build();
  }

  private void registerMXBean() {
    final Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.scmInfoBeanName = HddsUtils.registerWithJmxProperties(
        "StorageContainerManager", "StorageContainerManagerInfo",
        jmxProperties, this);
  }

  private void registerMetricsSource(SCMMXBean scmMBean) {
    scmContainerMetrics = SCMContainerMetrics.create(scmMBean);
  }

  private void unregisterMXBean() {
    if (this.scmInfoBeanName != null) {
      MBeans.unregister(this.scmInfoBeanName);
      this.scmInfoBeanName = null;
    }
  }

  @VisibleForTesting
  public ContainerInfo getContainerInfo(long containerID) throws
      IOException {
    return containerManager.getContainer(ContainerID.valueOf(containerID));
  }

  /**
   * Returns listening address of StorageLocation Protocol RPC server.
   *
   * @return listen address of StorageLocation RPC server
   */
  @VisibleForTesting
  public InetSocketAddress getClientRpcAddress() {
    return getClientProtocolServer().getClientRpcAddress();
  }

  @Override
  public String getClientRpcPort() {
    InetSocketAddress addr = getClientRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  public String getBlockProtocolRpcPort() {
    InetSocketAddress addr = getBlockProtocolServer().getBlockRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  public String getSecurityProtocolRpcPort() {
    InetSocketAddress addr = getSecurityProtocolServer().getRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  /**
   * Returns listening address of StorageDatanode Protocol RPC server.
   *
   * @return Address where datanode are communicating.
   */
  @Override
  public InetSocketAddress getDatanodeRpcAddress() {
    return getDatanodeProtocolServer().getDatanodeRpcAddress();
  }

  @Override
  public SCMNodeDetails getScmNodeDetails() {
    return scmHANodeDetails.getLocalNodeDetails();
  }

  public SCMHANodeDetails getSCMHANodeDetails() {
    return scmHANodeDetails;
  }

  @Override
  public String getDatanodeRpcPort() {
    InetSocketAddress addr = getDatanodeRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  /**
   * Start service.
   */
  @Override
  public void start() throws IOException {
    finalizationManager.runPrefinalizeStateActions();

    if (LOG.isInfoEnabled()) {
      LOG.info(buildRpcServerStartMessage(
          "StorageContainerLocationProtocol RPC server",
          getClientRpcAddress()));
    }

    scmHAManager.start();
    startSecretManagerIfNecessary();

    ms = HddsServerUtil
        .initializeMetrics(configuration, "StorageContainerManager");

    commandWatcherLeaseManager.start();
    getClientProtocolServer().start();

    if (LOG.isInfoEnabled()) {
      LOG.info(buildRpcServerStartMessage("ScmBlockLocationProtocol RPC " +
          "server", getBlockProtocolServer().getBlockRpcAddress()));
    }
    getBlockProtocolServer().start();

    // If HA is enabled, start datanode protocol server once leader is ready.
    if (!scmStorageConfig.isSCMHAEnabled()) {
      getDatanodeProtocolServer().start();
    }
    if (getSecurityProtocolServer() != null) {
      getSecurityProtocolServer().start();
      persistSCMCertificates();
    }

    scmBlockManager.start();

    // Start jvm monitor
    jvmPauseMonitor = new JvmPauseMonitor();
    jvmPauseMonitor.init(configuration);
    jvmPauseMonitor.start();

    try {
      httpServer = new StorageContainerManagerHttpServer(configuration, this);
      httpServer.start();
    } catch (Exception ex) {
      // SCM HttpServer start-up failure should be non-fatal
      LOG.error("SCM HttpServer failed to start.", ex);
    }

    setStartTime();
  }

  /** Persist SCM certs to DB on bootstrap scm nodes.
   *
   * @throws IOException
   */
  private void persistSCMCertificates() throws IOException {
    // Fetch all CA's and persist during startup on bootstrap nodes. This
    // is primarily being done to persist primary SCM Cert and Root CA.
    // TODO: see if we can avoid doing this during every restart.
    if (primaryScmNodeId != null && !primaryScmNodeId.equals(
        scmStorageConfig.getScmId())) {
      List<String> pemEncodedCerts =
          scmCertificateClient.listCA();

      // Write the primary SCM CA and Root CA during startup.
      for (String cert : pemEncodedCerts) {
        try {
          X509Certificate x509Certificate =
              CertificateCodec.getX509Certificate(cert);
          if (certificateStore.getCertificateByID(
              x509Certificate.getSerialNumber(), VALID_CERTS) == null) {
            LOG.info("Persist certificate serialId {} on Scm Bootstrap Node " +
                    "{}", x509Certificate.getSerialNumber(),
                scmStorageConfig.getScmId());
            certificateStore.storeValidScmCertificate(
                x509Certificate.getSerialNumber(), x509Certificate);
          }
        } catch (CertificateException ex) {
          LOG.error("Error while decoding CA Certificate", ex);
          throw new IOException(ex);
        }
      }
    }
  }

  /**
   * Stop service.
   */
  @Override
  public void stop() {
    try {
      if (containerBalancer.isBalancerRunning()) {
        LOG.info("Stopping Container Balancer service.");
        // stop ContainerBalancer thread in this scm
        containerBalancer.stop();
      } else {
        LOG.info("Container Balancer is not running.");
      }
    } catch (Exception e) {
      LOG.error("Failed to stop Container Balancer service.", e);
    }

    try {
      LOG.info("Stopping Replication Manager Service.");
      replicationManager.stop();
    } catch (Exception ex) {
      LOG.error("Replication manager service stop failed.", ex);
    }

    try {
      LOG.info("Stopping the Datanode Admin Monitor.");
      scmDecommissionManager.stop();
    } catch (Exception ex) {
      LOG.error("The Datanode Admin Monitor failed to stop", ex);
    }

    try {
      LOG.info("Stopping Lease Manager of the command watchers");
      commandWatcherLeaseManager.shutdown();
    } catch (Exception ex) {
      LOG.error("Lease Manager of the command watchers stop failed");
    }

    try {
      LOG.info("Stopping datanode service RPC server");
      getDatanodeProtocolServer().stop();

    } catch (Exception ex) {
      LOG.error("Storage Container Manager datanode RPC stop failed.", ex);
    }

    try {
      LOG.info("Stopping block service RPC server");
      getBlockProtocolServer().stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager blockRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping the StorageContainerLocationProtocol RPC server");
      getClientProtocolServer().stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager clientRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping Storage Container Manager HTTP server.");
      httpServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager HTTP server stop failed.", ex);
    }

    LOG.info("Stopping SCM LayoutVersionManager Service.");
    scmLayoutVersionManager.close();

    if (getSecurityProtocolServer() != null) {
      getSecurityProtocolServer().stop();
    }

    try {
      LOG.info("Stopping Block Manager Service.");
      scmBlockManager.stop();
    } catch (Exception ex) {
      LOG.error("SCM block manager service stop failed.", ex);
    }

    if (containerReportCache != null) {
      containerReportCache.invalidateAll();
      containerReportCache.cleanUp();
    }

    stopSecretManager();

    if (metrics != null) {
      metrics.unRegister();
    }

    unregisterMXBean();
    if (scmContainerMetrics != null) {
      scmContainerMetrics.unRegister();
    }
    if (placementMetrics != null) {
      placementMetrics.unRegister();
    }

    // Event queue must be stopped before the DB store is closed at the end.
    try {
      LOG.info("Stopping SCM Event Queue.");
      eventQueue.close();
    } catch (Exception ex) {
      LOG.error("SCM Event Queue stop failed", ex);
    }

    if (jvmPauseMonitor != null) {
      jvmPauseMonitor.stop();
    }

    try {
      LOG.info("Stopping SCM HA services.");
      scmHAManager.stop();
    } catch (Exception ex) {
      LOG.error("SCM HA Manager stop failed", ex);
    }

    IOUtils.cleanupWithLogger(LOG, containerManager);
    IOUtils.cleanupWithLogger(LOG, pipelineManager);

    try {
      LOG.info("Stopping SCM MetadataStore.");
      scmMetadataStore.stop();
    } catch (Exception ex) {
      LOG.error("SCM Metadata store stop failed", ex);
    }

    if (ms != null) {
      ms.stop();
    }

    scmSafeModeManager.stop();
  }

  @Override
  public void shutDown(String message) {
    stop();
    ExitUtils.terminate(1, message, LOG);
  }

  /**
   * Wait until service has completed shutdown.
   */
  @Override
  public void join() {
    try {
      getBlockProtocolServer().join();
      getClientProtocolServer().join();
      getDatanodeProtocolServer().join();
      if (getSecurityProtocolServer() != null) {
        getSecurityProtocolServer().join();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestate Healthy, Dead etc.
   * @return int -- count
   */
  public int getNodeCount(NodeState nodestate) {
    // TODO - decomm - this probably needs to accept opState and health
    return scmNodeManager.getNodeCount(null, nodestate);
  }

  /**
   * Returns the node decommission manager.
   *
   * @return NodeDecommissionManager The decommission manger for the used by
   *         scm
   */
  public NodeDecommissionManager getScmDecommissionManager() {
    return scmDecommissionManager;
  }

  /**
   * Returns SCMHAManager.
   */
  public SCMHAManager getScmHAManager() {
    return scmHAManager;
  }

  /**
   * Returns the Writable Container Factory.
   *
   * @return The WritableContainerFactory instance used by SCM.
   */
  public WritableContainerFactory getWritableContainerFactory() {
    return writableContainerFactory;
  }

  /**
   * Returns SCM container manager.
   */
  @VisibleForTesting
  @Override
  public ContainerManager getContainerManager() {
    return containerManager;
  }

  /**
   * Returns node manager.
   *
   * @return - Node Manager
   */
  @VisibleForTesting
  @Override
  public NodeManager getScmNodeManager() {
    return scmNodeManager;
  }

  /**
   * Returns pipeline manager.
   *
   * @return - Pipeline Manager
   */
  @VisibleForTesting
  @Override
  public PipelineManager getPipelineManager() {
    return pipelineManager;
  }

  @VisibleForTesting
  @Override
  public BlockManager getScmBlockManager() {
    return scmBlockManager;
  }

  @VisibleForTesting
  public SCMSafeModeManager getScmSafeModeManager() {
    return scmSafeModeManager;
  }

  @Override
  public ReplicationManager getReplicationManager() {
    return replicationManager;
  }

  public PlacementPolicy getContainerPlacementPolicy() {
    return containerPlacementPolicy;
  }

  public PlacementPolicyValidateProxy getPlacementPolicyValidateProxy() {
    return placementPolicyValidateProxy;
  }

  @VisibleForTesting
  @Override
  public ContainerBalancer getContainerBalancer() {
    return containerBalancer;
  }

  /**
   * Check if the current scm is the leader and ready for accepting requests.
   * @return - if the current scm is the leader and is ready.
   */
  public boolean checkLeader() {
    // For NON-HA setup, the node will always be the leader
    if (!SCMHAUtils.isSCMHAEnabled(configuration)) {
      return true;
    } else {
      // FOR HA setup, the node has to be the leader and ready to serve
      // requests.
      return getScmHAManager().getRatisServer().getDivision().getInfo()
          .isLeaderReady();
    }
  }

  public void checkAdminAccess(UserGroupInformation remoteUser)
      throws IOException {
    if (remoteUser != null && !scmAdmins.isAdmin(remoteUser)) {
      throw new AccessControlException(
          "Access denied for user " + remoteUser.getUserName() +
              ". Superuser privilege is required.");
    }
  }

  /**
   * Invalidate container stat entry for given datanode.
   *
   * @param datanodeUuid
   */
  public void removeContainerReport(String datanodeUuid) {
    synchronized (containerReportCache) {
      containerReportCache.invalidate(datanodeUuid);
    }
  }

  /**
   * Get container stat of specified datanode.
   *
   * @param datanodeUuid
   * @return
   */
  public ContainerStat getContainerReport(String datanodeUuid) {
    ContainerStat stat = null;
    synchronized (containerReportCache) {
      stat = containerReportCache.getIfPresent(datanodeUuid);
    }

    return stat;
  }

  /**
   * Returns a view of the container stat entries. Modifications made to the
   * map will directly
   * affect the cache.
   *
   * @return
   */
  public ConcurrentMap<String, ContainerStat> getContainerReportCache() {
    return containerReportCache.asMap();
  }

  @Override
  public Map<String, String> getContainerReport() {
    Map<String, String> id2StatMap = new HashMap<>();
    synchronized (containerReportCache) {
      ConcurrentMap<String, ContainerStat> map = containerReportCache.asMap();
      for (Map.Entry<String, ContainerStat> entry : map.entrySet()) {
        id2StatMap.put(entry.getKey(), entry.getValue().toJsonString());
      }
    }

    return id2StatMap;
  }

  /**
   * Returns live safe mode container threshold.
   *
   * @return String
   */
  @Override
  public double getSafeModeCurrentContainerThreshold() {
    return getCurrentContainerThreshold();
  }

  /**
   * Returns safe mode status.
   * @return boolean
   */
  @Override
  public boolean isInSafeMode() {
    return scmSafeModeManager.getInSafeMode();
  }

  /**
   * Returns EventPublisher.
   */
  public EventPublisher getEventQueue() {
    return eventQueue;
  }

  /**
   * Returns SCMContext.
   */
  public SCMContext getScmContext() {
    return scmContext;
  }

  /**
   * Returns SequenceIdGen.
   */
  public SequenceIdGenerator getSequenceIdGen() {
    return sequenceIdGen;
  }

  /**
   * Returns SCMServiceManager.
   */
  public SCMServiceManager getSCMServiceManager() {
    return serviceManager;
  }

  /**
   * Force SCM out of safe mode.
   */
  public boolean exitSafeMode() {
    scmSafeModeManager.exitSafeMode(eventQueue);
    return true;
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    return scmSafeModeManager.getCurrentContainerThreshold();
  }

  @Override
  public Map<String, Integer> getContainerStateCount() {
    Map<String, Integer> nodeStateCount = new HashMap<>();
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      nodeStateCount.put(state.toString(),
          containerManager.getContainerStateCount(state));
    }
    return nodeStateCount;
  }

  /**
   * Returns the SCM metadata Store.
   * @return SCMMetadataStore
   */
  public SCMMetadataStore getScmMetadataStore() {
    return scmMetadataStore;
  }

  /**
   * Returns the SCM network topology cluster.
   * @return NetworkTopology
   */
  public NetworkTopology getClusterMap() {
    return this.clusterMap;
  }

  public StatefulServiceStateManager getStatefulServiceStateManager() {
    return statefulServiceStateManager;
  }

  /**
   * Get the safe mode status of all rules.
   *
   * @return map of rule statuses.
   */
  public Map<String, Pair<Boolean, String>> getRuleStatus() {
    return scmSafeModeManager.getRuleStatus();
  }

  @Override
  public Map<String, String[]> getSafeModeRuleStatus() {
    Map<String, String[]> map = new HashMap<>();
    for (Map.Entry<String, Pair<Boolean, String>> entry :
        scmSafeModeManager.getRuleStatus().entrySet()) {
      String[] status =
          {entry.getValue().getRight(), entry.getValue().getLeft().toString()};
      map.put(entry.getKey(), status);
    }
    return map;
  }

  public PipelineChoosePolicy getPipelineChoosePolicy() {
    return this.pipelineChoosePolicy;
  }

  @Override
  public String getScmId() {
    return getScmStorageConfig().getScmId();
  }

  @Override
  public String getClusterId() {
    return getScmStorageConfig().getClusterID();
  }

  public HDDSLayoutVersionManager getLayoutVersionManager() {
    return scmLayoutVersionManager;
  }

  public FinalizationManager getFinalizationManager() {
    return finalizationManager;
  }

  /**
   * Return the node Id of this SCM.
   * @return node Id.
   */
  public String getSCMNodeId() {
    return scmHANodeDetails.getLocalNodeDetails().getNodeId();
  }

  private void startSecretManagerIfNecessary() {
    boolean shouldRun = securityConfig.isSecurityEnabled()
        && securityConfig.isContainerTokenEnabled()
        && containerTokenMgr != null;
    if (shouldRun) {
      boolean running = containerTokenMgr.isRunning();
      if (!running) {
        startSecretManager();
      }
    }
  }

  private void startSecretManager() {
    try {
      scmCertificateClient.assertValidKeysAndCertificate();
    } catch (OzoneSecurityException e) {
      LOG.error("Unable to read key pair.", e);
      throw new UncheckedIOException(e);
    }
    try {
      LOG.info("Starting token manager");
      containerTokenMgr.start(scmCertificateClient);
    } catch (IOException e) {
      // Unable to start secret manager.
      LOG.error("Error starting block token secret manager.", e);
      throw new UncheckedIOException(e);
    }
  }

  private void stopSecretManager() {
    if (containerTokenMgr != null) {
      LOG.info("Stopping block token manager.");
      try {
        containerTokenMgr.stop();
      } catch (IOException e) {
        LOG.error("Failed to stop block token manager", e);
      }
    }
  }

  public ContainerTokenGenerator getContainerTokenGenerator() {
    return containerTokenMgr != null
        ? containerTokenMgr
        : ContainerTokenGenerator.DISABLED;
  }

  @Override
  public String getScmRatisRoles() throws IOException {
    final SCMRatisServer server = getScmHAManager().getRatisServer();
    return server != null ?
        HddsUtils.format(server.getRatisRoles()) : "STANDALONE";
  }

  /**
   * @return hostname of primordialNode
   */
  @Override
  public String getPrimordialNode() {
    if (SCMHAUtils.isSCMHAEnabled(configuration)) {
      String primordialNode = SCMHAUtils.getPrimordialSCM(configuration);
      // primordialNode can be nodeId too . If it is then return hostname.
      if (SCMHAUtils.getSCMNodeIds(configuration).contains(primordialNode)) {
        List<SCMNodeDetails> localAndPeerNodes =
            new ArrayList<>(scmHANodeDetails.getPeerNodeDetails());
        localAndPeerNodes.add(getSCMHANodeDetails().getLocalNodeDetails());
        for (SCMNodeDetails nodes : localAndPeerNodes) {
          if (nodes.getNodeId().equals(primordialNode)) {
            return nodes.getHostName();
          }
        }

      }
      return primordialNode;
    }
    return null;
  }

  @Override
  public String getRatisLogDirectory() {
    return  SCMHAUtils.getSCMRatisDirectory(configuration);
  }

  @Override
  public String getRocksDbDirectory() {
    return String.valueOf(ServerUtils.getScmDbDir(configuration));
  }

}
