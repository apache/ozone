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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmUtils.checkIfCertSignRequestAllowed;
import static org.apache.hadoop.hdds.scm.security.SecretKeyManagerService.isSecretKeyEnable;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA_PREFIX;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.ObjectName;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.BlockManagerImpl;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerActionsHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.balancer.MoveManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMPerformanceMetrics;
import org.apache.hadoop.hdds.scm.container.reconciliation.ReconcileContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.container.replication.DatanodeCommandCountUpdatedHandler;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManagerEventHandler;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.BackgroundSCMService;
import org.apache.hadoop.hdds.scm.ha.HASecurityUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMHAMetrics;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMServiceException;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.DeadNodeHandler;
import org.apache.hadoop.hdds.scm.node.HealthyReadOnlyNodeHandler;
import org.apache.hadoop.hdds.scm.node.NewNodeHandler;
import org.apache.hadoop.hdds.scm.node.NodeAddressUpdateHandler;
import org.apache.hadoop.hdds.scm.node.NodeDecommissionManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeReportHandler;
import org.apache.hadoop.hdds.scm.node.ReadOnlyHealthyToHealthyNodeHandler;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.StaleNodeHandler;
import org.apache.hadoop.hdds.scm.node.StartDatanodeAdminHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineActionHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineReportHandler;
import org.apache.hadoop.hdds.scm.pipeline.WritableContainerFactory;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.PipelineChoosePolicyFactory;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.security.RootCARotationManager;
import org.apache.hadoop.hdds.scm.security.SecretKeyManagerService;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReport;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.security.token.ContainerTokenGenerator;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor;
import org.apache.hadoop.hdds.server.http.RatisDropwizardExports;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdds.utils.NettyMetrics;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.lease.LeaseManagerNotRunningException;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static SCMPerformanceMetrics perfMetrics;
  private SCMHAMetrics scmHAMetrics;
  private final NettyMetrics nettyMetrics;

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
  private SCMStorageConfig scmStorageConfig;
  private NodeDecommissionManager scmDecommissionManager;
  private WritableContainerFactory writableContainerFactory;
  private FinalizationManager finalizationManager;
  private HDDSLayoutVersionManager scmLayoutVersionManager;
  private LeaseManager<Object> leaseManager;

  private SCMMetadataStore scmMetadataStore;
  private CertificateStore certificateStore;
  private SCMHAManager scmHAManager;
  private SCMContext scmContext;
  private SequenceIdGenerator sequenceIdGen;

  private final EventQueue eventQueue;
  private final SCMServiceManager serviceManager;
  private final ReconfigurationHandler reconfigurationHandler;

  /*
   * HTTP endpoint for JMX access.
   */
  private StorageContainerManagerHttpServer httpServer;
  /**
   * SCM super user.
   */
  private final String scmStarterUser;
  private final OzoneAdmins scmAdmins;
  private final OzoneAdmins scmReadOnlyAdmins;

  /**
   * SCM mxbean.
   */
  private ObjectName scmInfoBeanName;

  private ReplicationManager replicationManager;

  private SCMSafeModeManager scmSafeModeManager;
  private SCMCertificateClient scmCertificateClient;
  private RootCARotationManager rootCARotationManager;
  private ContainerTokenSecretManager containerTokenMgr;

  private OzoneConfiguration configuration;
  private SCMContainerMetrics scmContainerMetrics;
  private SCMContainerPlacementMetrics placementMetrics;
  private PlacementPolicy containerPlacementPolicy;
  private PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private MetricsSystem ms;
  private final Map<String, RatisDropwizardExports> ratisMetricsMap =
      new ConcurrentHashMap<>();
  private List<RatisDropwizardExports.MetricReporter> ratisReporterList = null;
  private final String primaryScmNodeId;

  /**
   *  Network topology Map.
   */
  private NetworkTopology clusterMap;
  private PipelineChoosePolicy pipelineChoosePolicy;
  private PipelineChoosePolicy ecPipelineChoosePolicy;
  private SecurityConfig securityConfig;

  private final SCMHANodeDetails scmHANodeDetails;
  private final String threadNamePrefix;

  private final ContainerBalancer containerBalancer;
  // MoveManager is used by ContainerBalancer to schedule container moves
  private final MoveManager moveManager;
  private StatefulServiceStateManager statefulServiceStateManager;
  // Used to keep track of pending replication and pending deletes for
  // container replicas.
  private ContainerReplicaPendingOps containerReplicaPendingOps;
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private final SecretKeyManagerService secretKeyManagerService;

  private Clock systemClock;
  private DNSToSwitchMapping dnsToSwitchMapping;

  private String scmHostName;

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

    Objects.requireNonNull(configurator, "configurator cannot be null");
    Objects.requireNonNull(conf, "configuration cannot be null");
    // It is assumed the scm --init command creates the SCM Storage Config.
    scmStorageConfig = new SCMStorageConfig(conf);

    scmHANodeDetails = SCMHANodeDetails.loadSCMHAConfig(conf, scmStorageConfig);
    configuration = conf;
    initMetrics();
    initPerfMetrics();

    if (scmStorageConfig.getState() != StorageState.INITIALIZED) {
      String errMsg = "Please make sure you have run 'ozone scm --init' " +
          "command to generate all the required metadata to " +
          scmStorageConfig.getStorageDir() +
          " or make sure you have run 'ozone scm --bootstrap' cmd to " +
          "add the SCM to existing SCM HA group";
      LOG.error(errMsg + ".");
      throw new SCMException("SCM not initialized due to storage config " +
          "failure.", ResultCodes.SCM_NOT_INITIALIZED);
    }

    // Initialize Ratis if needed.
    // This is for the clusters which got upgraded from older version of Ozone.
    // We enable Ratis by default.
    if (!scmStorageConfig.isSCMHAEnabled()) {
      // Since we have initialized Ratis, we have to reload StorageConfig
      scmStorageConfig = initializeRatis(conf);
    }

    threadNamePrefix = getScmNodeDetails().threadNamePrefix();
    primaryScmNodeId = scmStorageConfig.getPrimaryScmNodeId();

    /*
     * Important : This initialization sequence is assumed by some of our tests.
     * The testSecureOzoneCluster assumes that security checks have to be
     * passed before any artifacts like SCM DB is created. So please don't
     * add any other initialization above the Security checks please.
     */
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      loginAsSCMUserIfSecurityEnabled(scmHANodeDetails, conf);
    }
    initializeCertificateClient();

    // Creates the SCM DBs or opens them if it exists.
    // A valid pointer to the store is required by all the other services below.
    initalizeMetadataStore(conf, configurator);

    eventQueue = new EventQueue(threadNamePrefix);
    serviceManager = new SCMServiceManager();
    reconfigurationHandler =
        new ReconfigurationHandler("SCM", conf, this::checkAdminAccess)
            .register(OZONE_ADMINISTRATORS, this::reconfOzoneAdmins)
            .register(OZONE_READONLY_ADMINISTRATORS,
                this::reconfOzoneReadOnlyAdmins);

    reconfigurationHandler.setReconfigurationCompleteCallback(reconfigurationHandler.defaultLoggingCallback());

    initializeSystemManagers(conf, configurator);

    if (isSecretKeyEnable(securityConfig)) {
      secretKeyManagerService = new SecretKeyManagerService(scmContext, conf,
              scmHAManager.getRatisServer());
      serviceManager.register(secretKeyManagerService);
    } else {
      secretKeyManagerService = null;
    }

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

    scmStarterUser = UserGroupInformation.getCurrentUser().getShortUserName();
    scmAdmins = OzoneAdmins.getOzoneAdmins(scmStarterUser, conf);
    scmReadOnlyAdmins = OzoneAdmins.getReadonlyAdmins(conf);
    LOG.info("SCM start with adminUsers: {}", scmAdmins.getAdminUsernames());

    datanodeProtocolServer = new SCMDatanodeProtocolServer(conf, this,
        eventQueue, scmContext);
    blockProtocolServer = new SCMBlockProtocolServer(conf, this);
    clientProtocolServer = new SCMClientProtocolServer(conf, this,
        reconfigurationHandler);

    initializeEventHandlers();

    moveManager = new MoveManager(replicationManager, containerManager);
    containerReplicaPendingOps.registerSubscriber(moveManager);
    containerBalancer = new ContainerBalancer(this);

    // Emit initial safe mode status, as now handlers are registered.
    scmSafeModeManager.start();
    scmHostName = HddsUtils.getHostName(conf);

    registerMXBean();
    registerMetricsSource(this);
    this.nettyMetrics = NettyMetrics.create();
  }

  private void initializeEventHandlers() {
    long timeDuration = configuration.getTimeDuration(
        OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION,
        OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION_DEFAULT
            .getDuration(), TimeUnit.MILLISECONDS);
    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(
            pipelineManager, containerManager, scmContext,
            leaseManager, timeDuration);
    NodeReportHandler nodeReportHandler =
        new NodeReportHandler(scmNodeManager);
    PipelineReportHandler pipelineReportHandler =
        new PipelineReportHandler(
            scmSafeModeManager, pipelineManager, scmContext, configuration);
    CommandStatusReportHandler cmdStatusReportHandler =
        new CommandStatusReportHandler();

    NewNodeHandler newNodeHandler = new NewNodeHandler(pipelineManager,
        scmDecommissionManager, serviceManager);
    NodeAddressUpdateHandler nodeAddressUpdateHandler =
            new NodeAddressUpdateHandler(pipelineManager,
                    scmDecommissionManager, serviceManager);
    StaleNodeHandler staleNodeHandler =
        new StaleNodeHandler(scmNodeManager, pipelineManager);
    DeadNodeHandler deadNodeHandler = new DeadNodeHandler(scmNodeManager,
        pipelineManager, containerManager);
    StartDatanodeAdminHandler datanodeStartAdminHandler =
        new StartDatanodeAdminHandler(scmNodeManager, pipelineManager);
    ReadOnlyHealthyToHealthyNodeHandler readOnlyHealthyToHealthyNodeHandler =
        new ReadOnlyHealthyToHealthyNodeHandler(serviceManager);
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
        new PipelineActionHandler(pipelineManager, scmContext);

    ReplicationManagerEventHandler replicationManagerEventHandler =
        new ReplicationManagerEventHandler(replicationManager, scmContext);

    ReconcileContainerEventHandler reconcileContainerEventHandler =
        new ReconcileContainerEventHandler(containerManager, scmContext);

    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.RETRIABLE_DATANODE_COMMAND, scmNodeManager);
    eventQueue.addHandler(SCMEvents.NODE_REPORT, nodeReportHandler);
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND_COUNT_UPDATED,
        new DatanodeCommandCountUpdatedHandler(replicationManager));
    eventQueue.addHandler(SCMEvents.REPLICATION_MANAGER_NOTIFY,
        replicationManagerEventHandler);

    // Use the same executor for both ICR and FCR.
    // The Executor maps the event to a thread for DN.
    // Dispatcher should always dispatch FCR first followed by ICR
    // conf: ozone.scm.event.CONTAINER_REPORT_OR_INCREMENTAL_CONTAINER_REPORT
    // .queue.wait.threshold
    long waitQueueThreshold = configuration.getInt(
        ScmUtils.getContainerReportConfPrefix() + ".queue.wait.threshold",
        OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT);
    // conf: ozone.scm.event.CONTAINER_REPORT_OR_INCREMENTAL_CONTAINER_REPORT
    // .execute.wait.threshold
    long execWaitThreshold = configuration.getInt(
        ScmUtils.getContainerReportConfPrefix() + ".execute.wait.threshold",
        OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT);
    List<BlockingQueue<ContainerReport>> queues
        = ScmUtils.initContainerReportQueue(configuration);
    List<ThreadPoolExecutor> executors
        = FixedThreadPoolWithAffinityExecutor.initializeExecutorPool(
            threadNamePrefix, queues);
    Map<String, FixedThreadPoolWithAffinityExecutor> reportExecutorMap
        = new ConcurrentHashMap<>();
    FixedThreadPoolWithAffinityExecutor<ContainerReportFromDatanode,
        ContainerReport> containerReportExecutors =
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(SCMEvents.CONTAINER_REPORT,
                containerReportHandler),
            containerReportHandler, queues, eventQueue,
            ContainerReportFromDatanode.class, executors,
            reportExecutorMap);
    containerReportExecutors.setQueueWaitThreshold(waitQueueThreshold);
    containerReportExecutors.setExecWaitThreshold(execWaitThreshold);
    FixedThreadPoolWithAffinityExecutor<IncrementalContainerReportFromDatanode,
        ContainerReport> incrementalReportExecutors =
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(
                SCMEvents.INCREMENTAL_CONTAINER_REPORT,
                incrementalContainerReportHandler),
            incrementalContainerReportHandler, queues, eventQueue,
            IncrementalContainerReportFromDatanode.class, executors,
            reportExecutorMap);
    incrementalReportExecutors.setQueueWaitThreshold(waitQueueThreshold);
    incrementalReportExecutors.setExecWaitThreshold(execWaitThreshold);

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
    eventQueue.addHandler(SCMEvents.RECONCILE_CONTAINER, reconcileContainerEventHandler);

    scmNodeManager.registerSendCommandNotify(
        SCMCommandProto.Type.deleteBlocksCommand,
        scmBlockManager.getDeletedBlockLog()::onSent);
  }

  private void initializeCertificateClient() throws IOException {
    securityConfig = new SecurityConfig(configuration);
    if (OzoneSecurityUtil.isSecurityEnabled(configuration) &&
        scmStorageConfig.checkPrimarySCMIdInitialized()) {
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
          getScmSecurityClientWithMaxRetry(configuration, getCurrentUser());
      scmCertificateClient = new SCMCertificateClient(
          securityConfig, scmSecurityClient, scmStorageConfig.getScmId(),
          scmStorageConfig.getClusterID(),
          scmStorageConfig.getScmCertSerialId(),
          getScmAddress(scmHANodeDetails, configuration).getHostName());
    }
  }

  public OzoneConfiguration getConfiguration() {
    return configuration;
  }

  @VisibleForTesting
  public void setConfiguration(OzoneConfiguration conf) {
    this.configuration = conf;
  }

  /**
   * Create an SCM instance based on the supplied configuration.
   *
   * @param conf        HDDS configuration
   * @param configurator SCM configurator
   * @return SCM instance
   * @throws IOException on Failure,
   * @throws AuthenticationException
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
   * @throws IOException on Failure,
   * @throws AuthenticationException
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
    // Use SystemClock when data is persisted
    // and used again after system restarts.
    systemClock = Clock.system(ZoneOffset.UTC);

    if (configurator.getNetworkTopology() != null) {
      clusterMap = configurator.getNetworkTopology();
    } else {
      clusterMap = new NetworkTopologyImpl(conf);
    }
    // This needs to be done before initializing Ratis.
    ratisReporterList = RatisDropwizardExports
        .registerRatisMetricReporters(ratisMetricsMap, isStopped::get);
    if (configurator.getSCMHAManager() != null) {
      scmHAManager = configurator.getSCMHAManager();
    } else {
      scmHAManager = new SCMHAManagerImpl(conf, securityConfig, this);
    }

    if (configurator.getLeaseManager() != null) {
      leaseManager = configurator.getLeaseManager();
    } else {
      long timeDuration = conf.getTimeDuration(
          OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION,
          OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION_DEFAULT
              .getDuration(), TimeUnit.MILLISECONDS);
      leaseManager = new LeaseManager<>(threadNamePrefix, timeDuration);
    }

    scmLayoutVersionManager = new HDDSLayoutVersionManager(
        scmStorageConfig.getLayoutVersion());
    VersionedDatanodeFeatures.initialize(scmLayoutVersionManager);

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
      // non-leader of term 0, in safe mode, preCheck not completed.
      scmContext = new SCMContext.Builder()
          .setLeader(false)
          .setTerm(0)
          .setSafeModeStatus(SCMSafeModeManager.SafeModeStatus.INITIAL)
          .setSCM(this)
          .setThreadNamePrefix(threadNamePrefix)
          .setFinalizationCheckpoint(finalizationManager.getCheckpoint())
          .build();
    }

    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        conf.getClass(
            ScmConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, conf);
    dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));

    if (configurator.getScmNodeManager() != null) {
      scmNodeManager = configurator.getScmNodeManager();
    } else {
      scmNodeManager = new SCMNodeManager(conf, scmStorageConfig, eventQueue,
          clusterMap, scmContext, scmLayoutVersionManager,
          this::resolveNodeLocation);
    }

    placementMetrics = SCMContainerPlacementMetrics.create();
    containerPlacementPolicy =
        ContainerPlacementPolicyFactory.getPolicy(conf, scmNodeManager,
            clusterMap, true, placementMetrics);

    PlacementPolicy ecContainerPlacementPolicy = ContainerPlacementPolicyFactory.getECPolicy(
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
              systemClock
              );
    }

    finalizationManager.buildUpgradeContext(scmNodeManager, pipelineManager,
        scmContext);

    ReplicationManager.ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    containerReplicaPendingOps =
        new ContainerReplicaPendingOps(systemClock, rmConf);

    long containerReplicaOpScrubberIntervalMs = conf.getTimeDuration(
        ScmConfigKeys
            .OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        ScmConfigKeys
            .OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    long backgroundServiceSafemodeWaitMs = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);

    final String backgroundServiceName = "ExpiredContainerReplicaOpScrubber";
    BackgroundSCMService expiredContainerReplicaOpScrubber =
        new BackgroundSCMService.Builder().setClock(systemClock)
            .setScmContext(scmContext)
            .setServiceName(backgroundServiceName)
            .setIntervalInMillis(containerReplicaOpScrubberIntervalMs)
            .setWaitTimeInMillis(backgroundServiceSafemodeWaitMs)
            .setPeriodicalTask(() -> containerReplicaPendingOps
                .removeExpiredEntries()).build();

    serviceManager.register(expiredContainerReplicaOpScrubber);

    if (configurator.getContainerManager() != null) {
      containerManager = configurator.getContainerManager();
    } else {
      containerManager = new ContainerManagerImpl(conf, scmHAManager,
          sequenceIdGen, pipelineManager, scmMetadataStore.getContainerTable(),
          containerReplicaPendingOps);
    }

    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    pipelineChoosePolicy = PipelineChoosePolicyFactory
        .getPolicy(scmNodeManager, scmConfig, false);
    ecPipelineChoosePolicy = PipelineChoosePolicyFactory
        .getPolicy(scmNodeManager, scmConfig, true);
    if (configurator.getWritableContainerFactory() != null) {
      writableContainerFactory = configurator.getWritableContainerFactory();
    } else {
      writableContainerFactory = new WritableContainerFactory(this);
    }
    if (configurator.getScmBlockManager() != null) {
      scmBlockManager = configurator.getScmBlockManager();
    } else {
      scmBlockManager = new BlockManagerImpl(conf, scmConfig, this);
    }
    if (configurator.getReplicationManager() != null) {
      replicationManager = configurator.getReplicationManager();
    }  else {
      replicationManager = new ReplicationManager(
          rmConf,
          conf,
          containerManager,
          containerPlacementPolicy,
          ecContainerPlacementPolicy,
          eventQueue,
          scmContext,
          scmNodeManager,
          systemClock,
          containerReplicaPendingOps);
      reconfigurationHandler.register(rmConf);
    }
    serviceManager.register(replicationManager);
    // RM gets notified of expired pending delete from containerReplicaPendingOps by subscribing to it
    // so it can resend them.
    containerReplicaPendingOps.registerSubscriber(replicationManager);
    if (configurator.getScmSafeModeManager() != null) {
      scmSafeModeManager = configurator.getScmSafeModeManager();
    } else {
      scmSafeModeManager = new SCMSafeModeManager(conf, scmNodeManager, pipelineManager,
          containerManager, serviceManager, eventQueue, scmContext);
    }

    scmDecommissionManager = new NodeDecommissionManager(conf, scmNodeManager, containerManager,
        scmContext, eventQueue, replicationManager);

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
            .setRatisServer(scmHAManager.getRatisServer()).build();


    final CertificateServer scmCertificateServer;
    final CertificateServer rootCertificateServer;

    // Start specific instance SCM CA server.
    String subject = SCM_SUB_CA_PREFIX +
        InetAddress.getLocalHost().getHostName();
    if (configurator.getCertificateServer() != null) {
      scmCertificateServer = configurator.getCertificateServer();
    } else {
      scmCertificateServer = new DefaultCAServer(subject,
          scmStorageConfig.getClusterID(), scmStorageConfig.getScmId(),
          certificateStore, null, new DefaultCAProfile(),
          scmCertificateClient.getComponentName());
      // INTERMEDIARY_CA which issues certs to DN and OM.
      scmCertificateServer.init(new SecurityConfig(configuration),
          CAType.SUBORDINATE);
    }

    // If primary SCM node Id is set it means this is a cluster which has
    // performed init with SCM HA version code.
    if (scmStorageConfig.checkPrimarySCMIdInitialized()) {
      if (primaryScmNodeId.equals(scmStorageConfig.getScmId())) {
        if (configurator.getCertificateServer() != null) {
          rootCertificateServer = configurator.getCertificateServer();
        } else {
          rootCertificateServer =
              HASecurityUtils.initializeRootCertificateServer(securityConfig,
                  certificateStore, scmStorageConfig, new DefaultCAProfile());
        }
        persistPrimarySCMCerts();
      } else {
        rootCertificateServer = null;
      }
    } else {
      // On an upgraded cluster primary scm nodeId will not be set as init will
      // not be run again after upgrade. For an upgraded cluster, besides one
      // intermediate CA server which is issuing certificates to DN and OM,
      // we will have one root CA server too.
      rootCertificateServer =
          HASecurityUtils.initializeRootCertificateServer(securityConfig,
              certificateStore, scmStorageConfig, new DefaultProfile());
    }

    SecretKeyManager secretKeyManager = secretKeyManagerService != null ?
        secretKeyManagerService.getSecretKeyManager() : null;

    // We need to pass getCACertificate as rootCA certificate,
    // as for SCM CA is root-CA.
    securityProtocolServer = new SCMSecurityProtocolServer(conf,
        rootCertificateServer,
        scmCertificateServer,
        scmCertificateClient,
        this,
        secretKeyManager);

    if (securityConfig.isContainerTokenEnabled()) {
      containerTokenMgr = createContainerTokenSecretManager();
    }
    if (securityConfig.isAutoCARotationEnabled()) {
      rootCARotationManager = new RootCARotationManager(this);
    }
  }

  /**
   * Persist primary SCM root ca cert and sub-ca certs to DB.
   */
  private void persistPrimarySCMCerts() throws IOException {
    BigInteger certSerial =
        scmCertificateClient.getCertificate().getSerialNumber();
    // Store the certificate in DB. On primary SCM when init happens, the
    // certificate is not persisted to DB. As we don't have Metadatstore
    // and ratis server initialized with statemachine. We need to do only
    // for primary scm, for other bootstrapped scm's certificates will be
    // persisted via ratis.
    if (certificateStore.getCertificateByID(certSerial) == null) {
      LOG.info("Storing sub-ca certificate serialId {} on primary SCM",
          certSerial);
      certificateStore.storeValidScmCertificate(
          certSerial, scmCertificateClient.getCertificate());
    }
    X509Certificate rootCACert = scmCertificateClient.getCACertificate();
    if (certificateStore.getCertificateByID(rootCACert.getSerialNumber()) == null) {
      LOG.info("Storing root certificate serialId {}",
          rootCACert.getSerialNumber());
      certificateStore.storeValidScmCertificate(
          rootCACert.getSerialNumber(), rootCACert);
    }
    // Upgrade certificate sequence ID
    SequenceIdGenerator.upgradeToCertificateSequenceId(scmMetadataStore, true);
  }

  public CertificateServer getRootCertificateServer() {
    return getSecurityProtocolServer().getRootCertificateServer();
  }

  public CertificateServer getScmCertificateServer() {
    return getSecurityProtocolServer().getScmCertificateServer();
  }

  public CertificateClient getScmCertificateClient() {
    return scmCertificateClient;
  }

  public Clock getSystemClock() {
    return systemClock;
  }

  private ContainerTokenSecretManager createContainerTokenSecretManager()
      throws IOException {

    long expiryTime = securityConfig.getBlockTokenExpiryDurationMs();

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
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
          getScmSecurityClientWithMaxRetry(configuration, getCurrentUser());
      scmCertificateClient = new SCMCertificateClient(securityConfig,
          scmSecurityClient, certSerialNumber, getScmId(),
          SCM_ROOT_CA_COMPONENT_NAME);
    }
    return new ContainerTokenSecretManager(expiryTime,
        secretKeyManagerService.getSecretKeyManager());
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
        ? String.format("%s is listening at %s", description, addr)
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
      int handlerCount,
      int readThreads)
      throws IOException {

    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setNumReaders(readThreads)
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
    String primordialSCM = SCMHAUtils.getPrimordialSCM(conf);
    SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    SCMHANodeDetails scmhaNodeDetails = SCMHANodeDetails.loadSCMHAConfig(conf,
        scmStorageConfig);
    String selfNodeId = scmhaNodeDetails.getLocalNodeDetails().getNodeId();
    final String selfHostName =
        scmhaNodeDetails.getLocalNodeDetails().getHostName();
    if (primordialSCM != null && SCMHAUtils.isPrimordialSCM(conf, selfNodeId, selfHostName)) {
      LOG.info("SCM bootstrap command can only be executed in non-Primordial SCM "
          + "{}, self id {} " + "Ignoring it.", primordialSCM, selfNodeId);
      return true;
    }

    loginAsSCMUserIfSecurityEnabled(scmhaNodeDetails, conf);

    final String persistedClusterId = scmStorageConfig.getClusterID();
    StorageState state = scmStorageConfig.getState();
    if (state == StorageState.INITIALIZED && conf
        .getBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
            ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_DEFAULT)) {
      LOG.info("Skipping clusterId validation during bootstrap command.  "
              + "ClusterId id {}, SCM id {}", persistedClusterId,
          scmStorageConfig.getScmId());

      // Initialize security if security is enabled later.
      initializeSecurityIfNeeded(conf, scmStorageConfig, selfHostName, false);

      return true;
    }

    // The node here will try to fetch the cluster id from any of existing
    // running SCM instances.
    OzoneConfiguration config =
        SCMHAUtils.removeSelfId(conf,
            scmhaNodeDetails.getLocalNodeDetails().getNodeId());
    final ScmInfo scmInfo = HAUtils.getScmInfo(config);
    final String fetchedId = scmInfo.getClusterId();
    Objects.requireNonNull(fetchedId, "fetchedId == null");
    if (state == StorageState.INITIALIZED) {
      Objects.requireNonNull(scmStorageConfig.getScmId(), "scmId == null");
      if (!fetchedId.equals(persistedClusterId)) {
        LOG.error(
            "Could not bootstrap as SCM is already initialized with cluster "
                + "id {} but cluster id for existing leader SCM instance "
                + "is {}", persistedClusterId, fetchedId);
        return false;
      }

      // Initialize security if security is enabled later.
      initializeSecurityIfNeeded(conf, scmStorageConfig, selfHostName, false);
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
              selfHostName, false);
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
   */
  private static void initializeSecurityIfNeeded(
      OzoneConfiguration conf, SCMStorageConfig scmStorageConfig,
      String scmHostname, boolean isPrimordial) throws IOException {
    // Initialize security if security is enabled later.
    if (OzoneSecurityUtil.isSecurityEnabled(conf)
        && scmStorageConfig.getScmCertSerialId() == null) {
      HASecurityUtils.initializeSecurity(scmStorageConfig, conf,
          scmHostname, isPrimordial);
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
    final String primordialSCM = SCMHAUtils.getPrimordialSCM(conf);
    final String selfNodeId = haDetails.getLocalNodeDetails().getNodeId();
    final String selfHostName = haDetails.getLocalNodeDetails().getHostName();
    if (primordialSCM != null &&
        !SCMHAUtils.isPrimordialSCM(conf, selfNodeId, selfHostName)) {
      LOG.info("SCM init command can only be executed on Primordial SCM. " +
          "Primordial SCM ID: {}. Self ID: {}.", primordialSCM, selfNodeId);
      return true;
    }
    if (state != StorageState.INITIALIZED) {
      try {
        if (clusterId != null && !clusterId.isEmpty()) {
          // clusterId must be an UUID
          Objects.requireNonNull(UUID.fromString(clusterId), "clusterId UUID == null");
          scmStorageConfig.setClusterId(clusterId);
        }


        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          HASecurityUtils.initializeSecurity(scmStorageConfig, conf,
              getScmAddress(haDetails, conf).getHostName(), true);
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
        scmStorageConfig = initializeRatis(conf);

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

      // Initialize security if security is enabled later.
      initializeSecurityIfNeeded(conf, scmStorageConfig, selfHostName, true);

      // Enable Ratis if it's not already enabled.
      if (!scmStorageConfig.isSCMHAEnabled()) {
        scmStorageConfig = initializeRatis(conf);

        /*
         * Since Ratis can be initialized on an existing cluster, we have to
         * trigger Ratis snapshot so that this SCM can send the latest scm.db
         * to the bootstrapping SCMs later.
         */
        try {
          StorageContainerManager scm = createSCM(conf);
          scm.start();
          scm.getScmHAManager().getRatisServer().triggerSnapshot();
          scm.stop();
          scm.join();
        } catch (AuthenticationException e) {
          throw new IOException(e);
        }
      }

      LOG.info("SCM already initialized. Reusing existing cluster id for sd={}"
              + ";cid={}; layoutVersion={}; HAEnabled={}",
          scmStorageConfig.getStorageDir(), scmStorageConfig.getClusterID(),
          scmStorageConfig.getLayoutVersion(), scmStorageConfig.isSCMHAEnabled());
      return true;
    }
  }

  private static SCMStorageConfig initializeRatis(OzoneConfiguration conf)
      throws IOException {
    final SCMStorageConfig storageConfig = new SCMStorageConfig(conf);
    final SCMHANodeDetails haDetails = SCMHANodeDetails.loadSCMHAConfig(conf, storageConfig);
    SCMRatisServerImpl.initialize(storageConfig.getClusterID(),
        storageConfig.getScmId(), haDetails.getLocalNodeDetails(), conf);
    storageConfig.setSCMHAFlag(true);
    storageConfig.setPrimaryScmNodeId(storageConfig.getScmId());
    storageConfig.forceInitialize();
    LOG.info("Enabled Ratis!");
    return storageConfig;
  }

  private static InetSocketAddress getScmAddress(SCMHANodeDetails haDetails,
      ConfigurationSource conf) throws IOException {
    List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(
        conf);
    Objects.requireNonNull(scmNodeInfoList, "scmNodeInfoList is null");

    InetSocketAddress scmAddress = null;
    if (HddsUtils.getScmServiceId(conf) != null) {
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

  /**
   * Initialize SCMPerformance metrics.
   */
  public static void initPerfMetrics() {
    perfMetrics = SCMPerformanceMetrics.create();
  }

  /**
   * Return SCMPerformance metrics instance.
   */
  public static SCMPerformanceMetrics getPerfMetrics() {
    return perfMetrics == null ? SCMPerformanceMetrics.create() : perfMetrics;
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

  public String threadNamePrefix() {
    return threadNamePrefix;
  }

  @Override
  public String getDatanodeRpcPort() {
    InetSocketAddress addr = getDatanodeRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  public CertificateStore getCertificateStore() {
    return certificateStore;
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

    ms = HddsServerUtil
        .initializeMetrics(configuration, "StorageContainerManager");

    getClientProtocolServer().start();

    if (LOG.isInfoEnabled()) {
      LOG.info(buildRpcServerStartMessage("ScmBlockLocationProtocol RPC " +
          "server", getBlockProtocolServer().getBlockRpcAddress()));
    }
    getBlockProtocolServer().start();

    // start datanode protocol server
    getDatanodeProtocolServer().start();
    if (getSecurityProtocolServer() != null) {
      getSecurityProtocolServer().start();
      persistSCMCertificates();
    }

    if (rootCARotationManager != null) {
      try {
        rootCARotationManager.start();
      } catch (SCMServiceException e) {
        throw new IOException("Failed to start root CA rotation manager", e);
      }
    }

    scmBlockManager.start();
    leaseManager.start();

    try {
      httpServer = new StorageContainerManagerHttpServer(configuration, this);
      httpServer.start();
    } catch (Exception ex) {
      // SCM HttpServer start-up failure should be non-fatal
      LOG.error("SCM HttpServer failed to start.", ex);
    }

    setStartTime();

    RaftPeerId leaderId = getScmHAManager().getRatisServer().getLeaderId();
    scmHAMetricsUpdate(Objects.toString(leaderId, null));

    if (scmCertificateClient != null) {
      // In case root CA certificate is rotated during this SCM is offline
      // period, fetch the new root CA list from leader SCM and refresh ratis
      // server's tlsConfig.
      scmCertificateClient.refreshCACertificates();
    }
  }

  /**
   * Persist SCM certs to DB on bootstrap scm nodes.
   */
  private void persistSCMCertificates() throws IOException {
    // Fetch all CA's and persist during startup on bootstrap nodes. This
    // is primarily being done to persist primary SCM Cert and Root CA.
    // TODO: see if we can avoid doing this during every restart.
    if (primaryScmNodeId != null && !primaryScmNodeId.equals(
        scmStorageConfig.getScmId())) {
      List<String> pemEncodedCerts =
          getScmSecurityClientWithMaxRetry(configuration, getCurrentUser()).listCACertificate();
      // Write the primary SCM CA and Root CA during startup.
      for (String cert : pemEncodedCerts) {
        final X509Certificate x509Certificate = CertificateCodec.readX509Certificate(cert);
        if (certificateStore.getCertificateByID(x509Certificate.getSerialNumber()) == null) {
          LOG.info("Persist certificate serialId {} on Scm Bootstrap Node " +
                  "{}", x509Certificate.getSerialNumber(),
              scmStorageConfig.getScmId());
          certificateStore.storeValidScmCertificate(
              x509Certificate.getSerialNumber(), x509Certificate);
        }
      }
    }
  }

  /**
   * Stop service.
   */
  @Override
  public void stop() {
    if (isStopped.getAndSet(true)) {
      LOG.info("Storage Container Manager is not running.");
      IOUtils.close(LOG, scmHAManager);
      stopReplicationManager(); // started eagerly
      return;
    }
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

    stopReplicationManager();

    try {
      LOG.info("Stopping the Datanode Admin Monitor.");
      scmDecommissionManager.stop();
    } catch (Exception ex) {
      LOG.error("The Datanode Admin Monitor failed to stop", ex);
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
      if (httpServer != null) {
        httpServer.stop();
      }
    } catch (Exception ex) {
      LOG.error("Storage Container Manager HTTP server stop failed.", ex);
    }

    LOG.info("Stopping SCM LayoutVersionManager Service.");
    scmLayoutVersionManager.close();

    if (getSecurityProtocolServer() != null) {
      getSecurityProtocolServer().stop();
    }

    if (rootCARotationManager != null) {
      rootCARotationManager.stop();
    }

    try {
      LOG.info("Stopping Block Manager Service.");
      scmBlockManager.stop();
    } catch (Exception ex) {
      LOG.error("SCM block manager service stop failed.", ex);
    }

    if (metrics != null) {
      metrics.unRegister();
    }

    nettyMetrics.unregister();
    if (perfMetrics != null) {
      perfMetrics.unRegister();
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

    try {
      LOG.info("Stopping SCM HA services.");
      scmHAManager.stop();
    } catch (Exception ex) {
      LOG.error("SCM HA Manager stop failed", ex);
    }

    if (scmHAMetrics != null) {
      SCMHAMetrics.unRegister();
    }

    IOUtils.cleanupWithLogger(LOG, pipelineManager);

    if (ms != null) {
      ms.stop();
    }

    scmSafeModeManager.stop();
    serviceManager.stop();
    try {
      leaseManager.shutdown();
    } catch (LeaseManagerNotRunningException ex) {
      LOG.debug("Lease manager not running, ignore");
    }
    RatisDropwizardExports.clear(ratisMetricsMap, ratisReporterList);

    if (scmCertificateClient != null) {
      try {
        scmCertificateClient.close();
      } catch (IOException ioe) {
        LOG.error("Closing certificate client failed", ioe);
      }
    }

    try {
      LOG.info("Stopping SCM MetadataStore.");
      scmMetadataStore.stop();
    } catch (Exception ex) {
      LOG.error("SCM Metadata store stop failed", ex);
    }
  }

  private void stopReplicationManager() {
    try {
      LOG.info("Stopping Replication Manager Service.");
      replicationManager.stop();
    } catch (Exception ex) {
      LOG.error("Replication manager service stop failed.", ex);
    }
  }

  @Override
  public void shutDown(String message) {
    stop();
    ExitUtils.terminate(0, message, LOG);
  }

  public boolean isStopped() {
    return isStopped.get();
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
  @Override
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

  @VisibleForTesting
  public SecretKeyManager getSecretKeyManager() {
    return secretKeyManagerService != null ?
        secretKeyManagerService.getSecretKeyManager() : null;
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

  public MoveManager getMoveManager() {
    return moveManager;
  }

  /**
   * Returns SCM root CA rotation manager.
   */
  public RootCARotationManager getRootCARotationManager() {
    return rootCARotationManager;
  }

  /**
   * Check if the current scm is the leader and ready for accepting requests.
   * @return - if the current scm is the leader and is ready.
   */
  public boolean checkLeader() {
    // The node has to be the leader and ready to serve requests.
    return getScmHAManager().getRatisServer().getDivision().getInfo()
          .isLeaderReady();
  }

  private void checkAdminAccess(String op) throws IOException {
    checkAdminAccess(getRemoteUser(), false);
  }

  public void checkAdminAccess(UserGroupInformation remoteUser, boolean isRead)
      throws IOException {
    if (remoteUser != null && !scmAdmins.isAdmin(remoteUser)) {
      if (!isRead || !scmReadOnlyAdmins.isAdmin(remoteUser)) {
        throw new AccessControlException(
            "Access denied for user " + remoteUser.getUserName() +
                ". SCM superuser privilege is required.");
      }
    }
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
  @Override
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
    scmSafeModeManager.forceExitSafeMode();
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
  @Override
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

  @Override
  public String getNamespace() {
    return scmHANodeDetails.getLocalNodeDetails().getServiceId();
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

  public PipelineChoosePolicy getEcPipelineChoosePolicy() {
    return this.ecPipelineChoosePolicy;
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

  @VisibleForTesting
  public SCMHAMetrics getScmHAMetrics() {
    return scmHAMetrics;
  }

  public SCMContainerPlacementMetrics getPlacementMetrics() {
    return placementMetrics;
  }

  public ContainerTokenGenerator getContainerTokenGenerator() {
    return containerTokenMgr != null
        ? containerTokenMgr
        : ContainerTokenGenerator.DISABLED;
  }

  @Override
  public List<List<String>> getScmRatisRoles() {
    final SCMRatisServer server = getScmHAManager().getRatisServer();

    // If Ratis is disabled
    if (server == null) {
      return getRatisRolesException("Ratis is disabled");
    }

    // To attempt to find the SCM Leader,
    // and if the Leader is not found
    // return Leader is not found message.
    RaftServer.Division division = server.getDivision();
    RaftPeerId leaderId = division.getInfo().getLeaderId();
    if (leaderId == null) {
      return getRatisRolesException("No leader found");
    }

    // If the SCMRatisServer is stopped, return a service stopped message.
    if (server.isStopped()) {
      return getRatisRolesException("Server is shutting down");
    }

    // Attempt to retrieve role information.
    try {
      List<String> ratisRoles = server.getRatisRoles();
      List<List<String>> result = new ArrayList<>();
      for (String role : ratisRoles) {
        String[] roleArr = role.split(":");
        List<String> scmInfo = new ArrayList<>();
        // Host Name
        scmInfo.add(roleArr[0]);
        // Node ID
        scmInfo.add(roleArr[3]);
        // Ratis Port
        scmInfo.add(roleArr[1]);
        // Role
        scmInfo.add(roleArr[2]);
        result.add(scmInfo);
      }
      return result;
    } catch (Exception e) {
      LOG.error("Failed to getRatisRoles.", e);
      return getRatisRolesException("Exception Occurred, " + e.getMessage());
    }
  }

  private static List<List<String>> getRatisRolesException(String exceptionString) {
    return Collections.singletonList(Collections.singletonList(exceptionString));
  }

  /**
   * @return hostname of primordialNode
   */
  @Override
  public String getPrimordialNode() {
    String primordialNode = SCMHAUtils.getPrimordialSCM(configuration);
    // primordialNode can be nodeId too . If it is then return hostname.
    if (HddsUtils.getSCMNodeIds(configuration).contains(primordialNode)) {
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

  @Override
  public String getRatisLogDirectory() {
    return  SCMHAUtils.getSCMRatisDirectory(configuration);
  }

  @Override
  public String getRocksDbDirectory() {
    return String.valueOf(ServerUtils.getScmDbDir(configuration));
  }

  @Override
  public String getHostname() {
    return scmHostName;
  }

  public Collection<String> getScmAdminUsernames() {
    return scmAdmins.getAdminUsernames();
  }

  public Collection<String> getScmReadOnlyAdminUsernames() {
    return scmReadOnlyAdmins.getAdminUsernames();
  }

  private String reconfOzoneAdmins(String newVal) {
    Collection<String> admins = OzoneAdmins.getOzoneAdminsFromConfigValue(
        newVal, scmStarterUser);
    scmAdmins.setAdminUsernames(admins);
    LOG.info("Load conf {} : {}, and now admins are: {}", OZONE_ADMINISTRATORS,
        newVal, admins);
    return String.valueOf(newVal);
  }

  private String reconfOzoneReadOnlyAdmins(String newVal) {
    Collection<String> admins = OzoneAdmins.getOzoneReadOnlyAdminsFromConfigValue(
        newVal);
    scmReadOnlyAdmins.setAdminUsernames(admins);
    LOG.info("Load conf {} : {}, and now read only admins are: {}",
        OZONE_READONLY_ADMINISTRATORS,
        newVal, admins);
    return String.valueOf(newVal);
  }

  /**
   * This will remove the given SCM node from HA Ring by removing it from
   * Ratis Ring.
   *
   * @return true if remove was successful, else false.
   */
  public boolean removePeerFromHARing(String scmId)
      throws IOException {

    if (getScmHAManager().getRatisServer() == null) {
      throw new IOException("Cannot remove SCM " +
          scmId + " in a non-HA cluster");
    }

    // We cannot remove a node if it's currently leader.
    if (scmContext.isLeader() && scmId.equals(getScmId())) {
      throw new IOException("Cannot remove current leader.");
    }

    checkIfCertSignRequestAllowed(rootCARotationManager, false, configuration,
        "removePeerFromHARing");

    Objects.requireNonNull(getScmHAManager().getRatisServer()
        .getDivision().getGroup(), "Group == null");

    // check valid scmid in ratis peers list
    if (getScmHAManager().getRatisServer().getDivision()
        .getGroup().getPeer(RaftPeerId.valueOf(scmId)) == null) {
      throw new IOException("ScmId " + scmId +
          " supplied for scm removal not in Ratis Peer list");
    }

    // create removeSCM request
    RemoveSCMRequest request = new RemoveSCMRequest(
        getClusterId(), scmId,
        getScmHAManager().getRatisServer().getDivision()
            .getGroup().getPeer(RaftPeerId.valueOf(scmId))
            .getAddress());

    return getScmHAManager().removeSCM(request);

  }

  /**
   * Check if the input scmId exists in the peers list.
   * @return true if the nodeId is self, or it exists in peer node list,
   *         false otherwise.
   */
  @VisibleForTesting
  public boolean doesPeerExist(String scmId) {
    if (getScmId().equals(scmId)) {
      return true;
    }
    return getScmHAManager().getRatisServer().getDivision()
        .getGroup().getPeer(RaftPeerId.valueOf(scmId)) != null;
  }

  public void scmHAMetricsUpdate(String leaderId) {
    // unregister, in case metrics already exist
    // so that the metric tags will get updated.
    SCMHAMetrics.unRegister();
    scmHAMetrics = SCMHAMetrics.create(getScmId(), leaderId);
  }

  @Override
  public ReconfigurationHandler getReconfigurationHandler() {
    return reconfigurationHandler;
  }

  public String resolveNodeLocation(String hostname) {
    List<String> hosts = Collections.singletonList(hostname);
    List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
    if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
      String location = resolvedHosts.get(0);
      LOG.debug("Node {} resolved to location {}", hostname, location);
      return location;
    } else {
      LOG.debug("Node resolution did not yield any result for {}", hostname);
      return null;
    }
  }

}
