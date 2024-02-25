/*
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

package org.apache.hadoop.ozone.recon.scm;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Singleton;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerActionsHandler;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.DeadNodeHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeReportHandler;
import org.apache.hadoop.hdds.scm.node.StaleNodeHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineActionHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.hadoop.ozone.common.DBUpdates;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.fsck.ContainerHealthTask;
import org.apache.hadoop.ozone.recon.fsck.ReconSafeModeMgrTask;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerSizeCountTask;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import com.google.inject.Inject;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.RECON_SCM_CONFIG_PREFIX;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.buildRpcServerStartMessage;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INTERVAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_SCM_DELTA_UPDATE_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_SCM_DELTA_UPDATE_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_SCM_DELTA_UPDATE_LOOP_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_SCM_DELTA_UPDATE_LOOP_LIMIT_DEFAULT;

import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReport;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;

import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.RocksDBUpdateEventBatch;
import org.apache.ratis.util.ExitUtils;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's 'lite' version of SCM.
 */
@Singleton
public class ReconStorageContainerManagerFacade
    implements OzoneStorageContainerManager {

  // TODO: Fix Recon.

  private static final Logger LOG = LoggerFactory
      .getLogger(ReconStorageContainerManagerFacade.class);
  public static final long CONTAINER_METADATA_SIZE = 1 * 1024 * 1024L;

  private final OzoneConfiguration ozoneConfiguration;
  private final ReconDatanodeProtocolServer datanodeProtocolServer;
  private final EventQueue eventQueue;
  private final SCMContext scmContext;
  private final SCMStorageConfig scmStorageConfig;
  private final SCMNodeDetails reconNodeDetails;
  private final SCMHAManager scmhaManager;
  private final SequenceIdGenerator sequenceIdGen;

  private DBStore dbStore;
  private ReconNodeManager nodeManager;
  private ReconPipelineManager pipelineManager;
  private ReconContainerManager containerManager;
  private NetworkTopology clusterMap;
  private StorageContainerServiceProvider scmServiceProvider;
  private Set<ReconScmTask> reconScmTasks = new HashSet<>();
  private SCMContainerPlacementMetrics placementMetrics;
  private PlacementPolicy containerPlacementPolicy;
  private HDDSLayoutVersionManager scmLayoutVersionManager;
  private ReconSafeModeManager safeModeManager;
  private ReconSafeModeMgrTask reconSafeModeMgrTask;
  private ContainerSizeCountTask containerSizeCountTask;
  private ContainerCountBySizeDao containerCountBySizeDao;
  private ScheduledExecutorService scheduler;
  private ScheduledExecutorService reconSCMSyncScheduler;
  private ReconTaskController reconTaskController;
  private ReconTaskStatusDao reconTaskStatusDao;
  private ReconScmMetadataManager scmMetadataManager;

  private long deltaUpdateLimit;
  private int deltaUpdateLoopLimit;
  private AtomicBoolean isSyncDataFromSCMRunning;
  private final String threadNamePrefix;
  private ThreadFactory threadFactory;

  // To Do :- Refactor the constructor in a separate JIRA
  @Inject
  @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:MethodLength"})
  public ReconStorageContainerManagerFacade(OzoneConfiguration conf,
      StorageContainerServiceProvider scmServiceProvider,
      ReconTaskStatusDao reconTaskStatusDao,
      ContainerCountBySizeDao containerCountBySizeDao,
      UtilizationSchemaDefinition utilizationSchemaDefinition,
      ContainerHealthSchemaManager containerHealthSchemaManager,
      ReconContainerMetadataManager reconContainerMetadataManager,
      ReconUtils reconUtils, ReconTaskController reconTaskController,
      ReconSafeModeManager safeModeManager,
      ReconScmMetadataManager scmMetadataManager) throws IOException {
    this.reconTaskController = reconTaskController;
    this.reconTaskStatusDao = reconTaskController.getReconTaskStatusDao();
    this.scmMetadataManager = scmMetadataManager;

    reconNodeDetails = reconUtils.getReconNodeDetails(conf);
    this.threadNamePrefix = reconNodeDetails.threadNamePrefix();
    this.eventQueue = new EventQueue(threadNamePrefix);
    eventQueue.setSilent(true);
    this.scmContext = new SCMContext.Builder()
        .setIsPreCheckComplete(true)
        .setSCM(this)
        .build();
    this.ozoneConfiguration = getReconScmConfiguration(conf);
    long scmClientRPCTimeOut = conf.getTimeDuration(
        OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_KEY,
        OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    long scmClientMaxRetryTimeOut = conf.getTimeDuration(
        OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_KEY,
        OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    int scmClientFailOverMaxRetryCount = conf.getInt(
        OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_KEY,
        OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_DEFAULT);

    conf.setLong(HDDS_SCM_CLIENT_RPC_TIME_OUT, scmClientRPCTimeOut);
    conf.setLong(HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT, scmClientMaxRetryTimeOut);
    conf.setLong(HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY,
        scmClientFailOverMaxRetryCount);

    long deltaUpdateLimits = ozoneConfiguration.getLong(RECON_SCM_DELTA_UPDATE_LIMIT,
        RECON_SCM_DELTA_UPDATE_LIMIT_DEFAULT);
    int deltaUpdateLoopLimits = ozoneConfiguration.getInt(RECON_SCM_DELTA_UPDATE_LOOP_LIMIT,
        RECON_SCM_DELTA_UPDATE_LOOP_LIMIT_DEFAULT);
    this.deltaUpdateLimit = deltaUpdateLimits;
    this.deltaUpdateLoopLimit = deltaUpdateLoopLimits;

    this.scmStorageConfig = new ReconStorageConfig(conf, reconUtils);
    this.clusterMap = new NetworkTopologyImpl(conf);

    this.dbStore = DBStoreBuilder.createDBStore(ozoneConfiguration, new ReconSCMDBDefinition());
    //checkAndInitializeSCMDBSnapshot(reconUtils);

    this.scmLayoutVersionManager =
        new HDDSLayoutVersionManager(scmStorageConfig.getLayoutVersion());
    this.scmhaManager = SCMHAManagerStub.getInstance(
        true, new SCMDBTransactionBufferImpl());
    this.sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, ReconSCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    this.nodeManager =
        new ReconNodeManager(conf, scmStorageConfig, eventQueue, clusterMap,
            ReconSCMDBDefinition.NODES.getTable(dbStore),
            this.scmLayoutVersionManager);
    placementMetrics = SCMContainerPlacementMetrics.create();
    this.containerPlacementPolicy =
        ContainerPlacementPolicyFactory.getPolicy(conf, nodeManager,
            clusterMap, true, placementMetrics);
    this.datanodeProtocolServer = new ReconDatanodeProtocolServer(
        conf, this, eventQueue);
    this.pipelineManager = ReconPipelineManager.newReconPipelineManager(
        conf, nodeManager,
        ReconSCMDBDefinition.PIPELINES.getTable(dbStore),
        eventQueue,
        scmhaManager,
        scmContext);
    ContainerReplicaPendingOps pendingOps = new ContainerReplicaPendingOps(
        Clock.system(ZoneId.systemDefault()));
    this.containerManager = new ReconContainerManager(conf,
        dbStore,
        ReconSCMDBDefinition.CONTAINERS.getTable(dbStore),
        pipelineManager, scmServiceProvider,
        containerHealthSchemaManager, reconContainerMetadataManager,
        scmhaManager, sequenceIdGen, pendingOps);

    scmMetadataManager.setOzoneStorageContainerManager(this);
    scmMetadataManager.setSequenceIdGen(sequenceIdGen);
    scmMetadataManager.setNodeManager(nodeManager);

    this.scmServiceProvider = scmServiceProvider;
    this.isSyncDataFromSCMRunning = new AtomicBoolean();
    this.containerCountBySizeDao = containerCountBySizeDao;
    NodeReportHandler nodeReportHandler =
        new NodeReportHandler(nodeManager);

    this.safeModeManager = safeModeManager;
    ReconPipelineReportHandler pipelineReportHandler =
        new ReconPipelineReportHandler(safeModeManager,
            pipelineManager, scmContext, conf, scmServiceProvider);

    PipelineActionHandler pipelineActionHandler =
        new PipelineActionHandler(pipelineManager, scmContext, conf);

    ReconTaskConfig reconTaskConfig = conf.getObject(ReconTaskConfig.class);
    PipelineSyncTask pipelineSyncTask = new PipelineSyncTask(
        pipelineManager,
        nodeManager,
        scmServiceProvider,
        reconTaskStatusDao,
        reconTaskConfig);
    ContainerHealthTask containerHealthTask = new ContainerHealthTask(
        containerManager, scmServiceProvider, reconTaskStatusDao,
        containerHealthSchemaManager, containerPlacementPolicy, reconTaskConfig,
        reconContainerMetadataManager, conf);

    this.containerSizeCountTask = new ContainerSizeCountTask(
        containerManager,
        scmServiceProvider,
        reconTaskStatusDao,
        reconTaskConfig,
        containerCountBySizeDao,
        utilizationSchemaDefinition);

    StaleNodeHandler staleNodeHandler =
        new ReconStaleNodeHandler(nodeManager, pipelineManager, conf,
            pipelineSyncTask);
    DeadNodeHandler deadNodeHandler = new ReconDeadNodeHandler(nodeManager,
        pipelineManager, containerManager, scmServiceProvider,
        containerHealthTask, pipelineSyncTask, containerSizeCountTask);

    ContainerReportHandler containerReportHandler =
        new ReconContainerReportHandler(nodeManager, containerManager);
    IncrementalContainerReportHandler icrHandler =
        new ReconIncrementalContainerReportHandler(nodeManager,
            containerManager, scmContext);
    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(
            pipelineManager, containerManager, scmContext,
            null, 0);
    ContainerActionsHandler actionsHandler = new ContainerActionsHandler();
    ReconNewNodeHandler newNodeHandler = new ReconNewNodeHandler(nodeManager);
    // Use the same executor for both ICR and FCR.
    // The Executor maps the event to a thread for DN.
    // Dispatcher should always dispatch FCR first followed by ICR
    // conf: ozone.scm.event.CONTAINER_REPORT_OR_INCREMENTAL_CONTAINER_REPORT
    // .queue.wait.threshold
    long waitQueueThreshold = ozoneConfiguration.getInt(
        ScmUtils.getContainerReportConfPrefix() + ".queue.wait.threshold",
        OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT);
    // conf: ozone.scm.event.CONTAINER_REPORT_OR_INCREMENTAL_CONTAINER_REPORT
    // .execute.wait.threshold
    long execWaitThreshold = ozoneConfiguration.getInt(
        ScmUtils.getContainerReportConfPrefix() + ".execute.wait.threshold",
        OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT);
    List<BlockingQueue<ContainerReport>> queues
        = ReconUtils.initContainerReportQueue(ozoneConfiguration);
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
                icrHandler),
            icrHandler, queues, eventQueue,
            IncrementalContainerReportFromDatanode.class, executors,
            reportExecutorMap);
    incrementalReportExecutors.setQueueWaitThreshold(waitQueueThreshold);
    incrementalReportExecutors.setExecWaitThreshold(execWaitThreshold);
    eventQueue.addHandler(SCMEvents.CONTAINER_REPORT, containerReportExecutors,
        containerReportHandler);
    eventQueue.addHandler(SCMEvents.INCREMENTAL_CONTAINER_REPORT,
        incrementalReportExecutors, icrHandler);
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, nodeManager);
    eventQueue.addHandler(SCMEvents.NODE_REPORT, nodeReportHandler);
    eventQueue.addHandler(SCMEvents.PIPELINE_REPORT, pipelineReportHandler);
    eventQueue.addHandler(SCMEvents.PIPELINE_ACTIONS, pipelineActionHandler);
    eventQueue.addHandler(SCMEvents.STALE_NODE, staleNodeHandler);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    eventQueue.addHandler(SCMEvents.CONTAINER_ACTIONS, actionsHandler);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    eventQueue.addHandler(SCMEvents.NEW_NODE, newNodeHandler);
    reconScmTasks.add(pipelineSyncTask);
    reconScmTasks.add(containerHealthTask);
    reconScmTasks.add(containerSizeCountTask);
    reconSafeModeMgrTask = new ReconSafeModeMgrTask(
        containerManager, nodeManager, safeModeManager,
        reconTaskConfig, ozoneConfiguration);
    this.threadFactory =
        new ThreadFactoryBuilder().setNameFormat(threadNamePrefix + "ReconSyncSCM-%d")
            .build();
  }

  private DBStore checkAndInitializeSCMDBSnapshot(ReconUtils reconUtils) throws IOException {
    File reconDbDir =
        reconUtils.getReconDbDir(ozoneConfiguration, OZONE_RECON_SCM_DB_DIR);
    File lastKnownSCMSnapshot =
        reconUtils.getLastKnownDB(reconDbDir, RECON_SCM_SNAPSHOT_DB);
    if (lastKnownSCMSnapshot != null) {
      LOG.info("Last known snapshot for SCM : {}", lastKnownSCMSnapshot.getAbsolutePath());
      return createDBAndAddSCMTablesAndCodecs(
          lastKnownSCMSnapshot, new ReconSCMDBDefinition());
    } else {
      return DBStoreBuilder
          .createDBStore(ozoneConfiguration, new ReconSCMDBDefinition());
    }
  }

  /**
   *  For every config key which is prefixed by 'recon.scmconfig', create a new
   *  config key without the prefix keeping the same value.
   *  For example, if recon.scm.a.b. = xyz, we add a new config like
   *  a.b.c = xyz. This is done to override Recon's passive SCM configs if
   *  needed.
   * @param configuration configuration object.
   * @return same configuration object with possible added elements.
   */
  private OzoneConfiguration getReconScmConfiguration(
      OzoneConfiguration configuration) {
    OzoneConfiguration reconScmConfiguration =
        new OzoneConfiguration(configuration);
    Map<String, String> reconScmConfigs =
        configuration.getPropsMatchPrefixAndTrimPrefix(RECON_SCM_CONFIG_PREFIX);
    for (Map.Entry<String, String> entry : reconScmConfigs.entrySet()) {
      reconScmConfiguration.set(entry.getKey(), entry.getValue());
    }
    return reconScmConfiguration;
  }

  /**
   * Start the Recon SCM subsystems.
   */
  @Override
  public void start() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info(buildRpcServerStartMessage(
          "Recon ScmDatanodeProtocol RPC server",
          getDatanodeProtocolServer().getDatanodeRpcAddress()));
    }
    reconSCMSyncScheduler = Executors.newScheduledThreadPool(1, threadFactory);
    registerSCMDBTasks();
    try {
      scmMetadataManager.start(ozoneConfiguration);
    } catch (IOException ioEx) {
      LOG.error("Error staring Recon SCM Metadata Manager.", ioEx);
    }
    reconTaskController.start();
    long initialDelay = ozoneConfiguration.getTimeDuration(
        OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY,
        ozoneConfiguration.get(
            ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
            OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT),
        TimeUnit.MILLISECONDS);

    // This schedules a periodic task to sync Recon's copy of SCM metadata
    // with SCM metadata rocks DB. Gets full snapshot or delta updates.
    scheduleSyncDataFromSCM(initialDelay);

    scheduler = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat(threadNamePrefix +
                "SyncSCMContainerInfo-%d")
            .build());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Started the SCM Container Info sync scheduler.");
    }
    long interval = ozoneConfiguration.getTimeDuration(
        OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INTERVAL_DELAY,
        OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    initialDelay = ozoneConfiguration.getTimeDuration(
        OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INITIAL_DELAY,
        OZONE_RECON_SCM_CONTAINER_INFO_SYNC_TASK_INITIAL_DELAY_DEFAULT,
        TimeUnit.MILLISECONDS);
    // This periodic sync with SCM container cache is needed because during
    // the window when recon will be down and any container being added
    // newly and went missing, that container will not be reported as missing by
    // recon till there is a difference of container count equivalent to
    // threshold value defined in "ozone.recon.scm.container.threshold"
    // between SCM container cache and recon container cache.
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        boolean isSuccess = syncWithSCMContainerInfo();
        if (!isSuccess) {
          LOG.debug("SCM container info sync is already running.");
        }
      } catch (Throwable t) {
        LOG.error("Unexpected exception while syncing data from SCM.", t);
      } finally {
        isSyncDataFromSCMRunning.compareAndSet(true, false);
      }
    },
        initialDelay,
        interval,
        TimeUnit.MILLISECONDS);
    getDatanodeProtocolServer().start();
    reconSafeModeMgrTask.start();
    if (!this.safeModeManager.getInSafeMode()) {
      this.reconScmTasks.forEach(ReconScmTask::start);
    }
  }

  public void registerSCMDBTasks() {
    String scmDeltaRequest = StorageContainerServiceProviderImpl.SCMSnapshotTaskName.SCMDeltaRequest.name();
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(scmDeltaRequest,
        System.currentTimeMillis(), getCurrentSCMDBSequenceNumber());
    if (!reconTaskStatusDao.existsById(scmDeltaRequest)) {
      reconTaskStatusDao.insert(reconTaskStatusRecord);
      LOG.info("Registered {} task ", scmDeltaRequest);
    }

    String scmSnapshotRequest = StorageContainerServiceProviderImpl.SCMSnapshotTaskName.SCMSnapshotRequest.name();
    reconTaskStatusRecord = new ReconTaskStatus(scmSnapshotRequest,
        System.currentTimeMillis(), getCurrentSCMDBSequenceNumber());
    if (!reconTaskStatusDao.existsById(scmSnapshotRequest)) {
      reconTaskStatusDao.insert(reconTaskStatusRecord);
      LOG.info("Registered {} task ", scmSnapshotRequest);
    }
  }

  private void scheduleSyncDataFromSCM(long initialDelay) {
    long interval = ozoneConfiguration.getTimeDuration(OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY,
        ozoneConfiguration.get(OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY,
            OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DEFAULT),
        TimeUnit.MILLISECONDS);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started the Recon SCM DB sync scheduler.");
    }
    reconSCMSyncScheduler.scheduleWithFixedDelay(() -> {
      try {
        boolean isSuccess = syncDataFromSCM();
        if (!isSuccess) {
          LOG.debug("Recon SCM DB sync is already running.");
        }
      } catch (Throwable t) {
        LOG.error("Unexpected exception while syncing data from SCM.", t);
      } finally {
        isSyncDataFromSCMRunning.compareAndSet(true, false);
      }
    },
        initialDelay,
        interval,
        TimeUnit.MILLISECONDS);
  }

  private void stopSyncDataFromSCMThread() {
    reconSCMSyncScheduler.shutdownNow();
    LOG.debug("Shutdown the Recon SCM DB sync scheduler.");
  }

  /**
   * Get Delta updates from SCM through RPC call and apply to local SCM DB as
   * well as accumulate in a buffer.
   * @param fromSequenceNumber from sequence number to request from.
   * @param scmdbUpdatesHandler SCM DB updates handler to buffer updates.
   * @throws IOException when SCM RPC request fails.
   * @throws RocksDBException when writing to RocksDB fails.
   */
  @VisibleForTesting
  void getAndApplyDeltaUpdatesFromSCM(
      long fromSequenceNumber, SCMDBUpdatesHandler scmdbUpdatesHandler)
      throws IOException, RocksDBException {
    int loopCount = 0;
    LOG.info("OriginalFromSequenceNumber : {} ", fromSequenceNumber);
    long deltaUpdateCnt = Long.MAX_VALUE;
    long inLoopStartSequenceNumber = fromSequenceNumber;
    long inLoopLatestSequenceNumber;
    while (loopCount < deltaUpdateLoopLimit &&
        deltaUpdateCnt >= deltaUpdateLimit) {
      if (!innerGetAndApplyDeltaUpdatesFromSCM(
          inLoopStartSequenceNumber, scmdbUpdatesHandler)) {
        LOG.error(
            "Retrieve SCM DB delta update failed for sequence number : {}, " +
                "so falling back to full snapshot.", inLoopStartSequenceNumber);
        throw new RocksDBException(
            "Unable to get delta updates since sequenceNumber - " +
                inLoopStartSequenceNumber);
      }
      inLoopLatestSequenceNumber = getCurrentSCMDBSequenceNumber();
      deltaUpdateCnt = inLoopLatestSequenceNumber - inLoopStartSequenceNumber;
      inLoopStartSequenceNumber = inLoopLatestSequenceNumber;
      loopCount++;
    }
    LOG.info("Delta updates received from SCM : {} loops, {} records", loopCount,
        getCurrentSCMDBSequenceNumber() - fromSequenceNumber
    );
  }

  /**
   * Get Delta updates from SCM through RPC call and apply to local SCM DB as
   * well as accumulate in a buffer.
   * @param fromSequenceNumber from sequence number to request from.
   * @param scmdbUpdatesHandler SCM DB updates handler to buffer updates.
   * @throws IOException when SCM RPC request fails.
   * @throws RocksDBException when writing to RocksDB fails.
   */
  @VisibleForTesting
  boolean innerGetAndApplyDeltaUpdatesFromSCM(long fromSequenceNumber,
                                              SCMDBUpdatesHandler scmdbUpdatesHandler)
      throws IOException, RocksDBException {
    StorageContainerLocationProtocolProtos.DBUpdatesRequestProto dbUpdatesRequest =
        StorageContainerLocationProtocolProtos.DBUpdatesRequestProto.newBuilder()
            .setSequenceNumber(fromSequenceNumber)
            .setLimitCount(deltaUpdateLimit)
            .build();
    DBUpdates dbUpdates = scmServiceProvider.getDBUpdates(dbUpdatesRequest);
    int numUpdates = 0;
    long latestSequenceNumberOfOM = -1L;
    if (null != dbUpdates && dbUpdates.getCurrentSequenceNumber() != -1) {
      latestSequenceNumberOfOM = dbUpdates.getLatestSequenceNumber();
      RDBStore rocksDBStore = (RDBStore) scmMetadataManager.getStore();
      final RocksDatabase rocksDB = rocksDBStore.getDb();
      numUpdates = dbUpdates.getData().size();
      for (byte[] data : dbUpdates.getData()) {
        try (ManagedWriteBatch writeBatch = new ManagedWriteBatch(data)) {
          writeBatch.iterate(scmdbUpdatesHandler);
          try (RDBBatchOperation rdbBatchOperation =
                   new RDBBatchOperation(writeBatch)) {
            try (ManagedWriteOptions wOpts = new ManagedWriteOptions()) {
              rdbBatchOperation.commit(rocksDB, wOpts);
            }
          }
        }
      }
    }
    long lag = latestSequenceNumberOfOM == -1 ? 0 :
        latestSequenceNumberOfOM - getCurrentSCMDBSequenceNumber();
    LOG.info("Number of updates received from SCM : {}, " +
            "SequenceNumber diff: {}, SequenceNumber Lag from SCM {}, " +
            "isDBUpdateSuccess: {}", numUpdates, getCurrentSCMDBSequenceNumber()
            - fromSequenceNumber, lag,
        null != dbUpdates && dbUpdates.isDBUpdateSuccess());
    return null != dbUpdates && dbUpdates.isDBUpdateSuccess();
  }

  /**
   * Based on current state of Recon's SCM DB, we either get delta updates or
   * full snapshot from SCM.
   */
  @VisibleForTesting
  public boolean syncDataFromSCM() {
    if (isSyncDataFromSCMRunning.compareAndSet(false, true)) {
      try {
        LOG.info("Syncing data from SCM.");
        long currentSequenceNumber = getCurrentSCMDBSequenceNumber();
        LOG.debug("Seq number of Recon's SCM DB : {}", currentSequenceNumber);
        boolean fullSnapshot = false;

        if (currentSequenceNumber <= 0) {
          fullSnapshot = true;
        } else {
          try (SCMDBUpdatesHandler scmdbUpdatesHandler =
                   new SCMDBUpdatesHandler(scmMetadataManager)) {
            LOG.info("Obtaining delta updates from SCM");
            // Get updates from SCM and apply to local Recon SCM DB.
            getAndApplyDeltaUpdatesFromSCM(currentSequenceNumber, scmdbUpdatesHandler);
            // Update timestamp of successful delta updates query.
            ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
                StorageContainerServiceProviderImpl.SCMSnapshotTaskName.SCMDeltaRequest.name(),
                System.currentTimeMillis(), getCurrentSCMDBSequenceNumber());
            reconTaskStatusDao.update(reconTaskStatusRecord);

            // Pass on DB update events to tasks that are listening.
            reconTaskController.consumeSCMEvents(new RocksDBUpdateEventBatch(
                scmdbUpdatesHandler.getEvents()), scmMetadataManager);
          } catch (InterruptedException intEx) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            LOG.warn("Unable to get and apply delta updates from SCM.",
                e.getMessage());
            fullSnapshot = true;
          }
        }

        if (fullSnapshot) {
          try {
            LOG.info("Obtaining full snapshot from SCM");
            // Update local Recon SCM DB to new snapshot.
            boolean success = scmServiceProvider.updateReconSCMDBWithNewSnapshot();
            // Update timestamp of successful delta updates query.
            if (success) {
              ReconTaskStatus reconTaskStatusRecord =
                  new ReconTaskStatus(
                      StorageContainerServiceProviderImpl.SCMSnapshotTaskName.SCMSnapshotRequest.name(),
                      System.currentTimeMillis(),
                      getCurrentSCMDBSequenceNumber());
              reconTaskStatusDao.update(reconTaskStatusRecord);

              // Reinitialize tasks that are listening.
              LOG.info("Calling reprocess on Recon SCM tasks.");
              reconTaskController.reInitializeSCMTasks(scmMetadataManager);
              // Reinitialize Recon managers with new DB store.
              dbStore = scmServiceProvider.getStore();
            }
          } catch (InterruptedException intEx) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            LOG.error("Unable to update Recon's metadata with new SCM DB. ", e);
          }
        }
      } finally {
        isSyncDataFromSCMRunning.compareAndSet(true, false);
      }
    } else {
      LOG.debug("SCM DB sync is already running.");
      return false;
    }
    return true;
  }

  /**
   * Wait until service has completed shutdown.
   */
  @Override
  public void join() {
    try {
      getDatanodeProtocolServer().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
  }

  /**
   * Stop the Recon SCM subsystems.
   */
  @Override
  public void stop() {
    getDatanodeProtocolServer().stop();
    reconScmTasks.forEach(ReconScmTask::stop);
    try {
      LOG.info("Stopping SCM Event Queue.");
      eventQueue.close();
    } catch (Exception ex) {
      LOG.error("SCM Event Queue stop failed", ex);
    }
    stopSyncDataFromSCMThread();
    reconTaskController.stop();
    try {
      scmMetadataManager.stop();
    } catch (Exception ex) {
      LOG.error("ReconScmMetaDataManager stop failed", ex);
    }
    scheduler.shutdownNow();
    IOUtils.cleanupWithLogger(LOG, nodeManager);
    IOUtils.cleanupWithLogger(LOG, containerManager);
    IOUtils.cleanupWithLogger(LOG, pipelineManager);
    LOG.info("Flushing container replica history to DB.");
    containerManager.flushReplicaHistoryMapToDB(true);
    IOUtils.close(LOG, dbStore);
    IOUtils.close(LOG, scmMetadataManager.getStore());
  }

  @Override
  public void shutDown(String message) {
    stop();
    ExitUtils.terminate(0, message, LOG);
  }

  public ReconDatanodeProtocolServer getDatanodeProtocolServer() {
    return datanodeProtocolServer;
  }

  public boolean syncWithSCMContainerInfo()
      throws IOException {
    if (isSyncDataFromSCMRunning.compareAndSet(false, true)) {
      try {
        List<ContainerInfo> containers = containerManager.getContainers();

        long totalContainerCount = scmServiceProvider.getContainerCount(
            HddsProtos.LifeCycleState.CLOSED);
        long containerCountPerCall =
            getContainerCountPerCall(totalContainerCount);
        long startContainerId = 1;
        long retrievedContainerCount = 0;
        if (totalContainerCount > 0) {
          while (retrievedContainerCount < totalContainerCount) {
            List<ContainerInfo> listOfContainers = scmServiceProvider.
                getListOfContainers(startContainerId,
                    Long.valueOf(containerCountPerCall).intValue(),
                    HddsProtos.LifeCycleState.CLOSED);
            if (null != listOfContainers && listOfContainers.size() > 0) {
              LOG.info("Got list of containers from SCM : " +
                  listOfContainers.size());
              listOfContainers.forEach(containerInfo -> {
                long containerID = containerInfo.getContainerID();
                boolean isContainerPresentAtRecon =
                    containers.contains(containerInfo);
                if (!isContainerPresentAtRecon) {
                  try {
                    ContainerWithPipeline containerWithPipeline =
                        scmServiceProvider.getContainerWithPipeline(
                            containerID);
                    containerManager.addNewContainer(containerWithPipeline);
                  } catch (IOException e) {
                    LOG.error("Could not get container with pipeline " +
                        "for container : {}", containerID);
                  }
                }
              });
              startContainerId = listOfContainers.get(
                  listOfContainers.size() - 1).getContainerID() + 1;
            } else {
              LOG.info("No containers found at SCM in CLOSED state");
              return false;
            }
            retrievedContainerCount += containerCountPerCall;
          }
        }
      } catch (IOException e) {
        LOG.error("Unable to refresh Recon SCM DB Snapshot. ", e);
        return false;
      }
    } else {
      LOG.debug("SCM DB sync is already running.");
      return false;
    }
    return true;
  }

  private long getContainerCountPerCall(long totalContainerCount) {
    // Assumption of size of 1 container info object here is 1 MB
    long containersMetaDataTotalRpcRespSizeMB =
        CONTAINER_METADATA_SIZE * totalContainerCount;
    long hadoopRPCSize = ozoneConfiguration.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    long containerCountPerCall = containersMetaDataTotalRpcRespSizeMB <=
        hadoopRPCSize ? totalContainerCount :
        Math.round(Math.floor(
            hadoopRPCSize / (double) CONTAINER_METADATA_SIZE));
    return containerCountPerCall;
  }

  @Override
  public NodeManager getScmNodeManager() {
    return nodeManager;
  }

  @Override
  public BlockManager getScmBlockManager() {
    return null;
  }

  @Override
  public PipelineManager getPipelineManager() {
    return pipelineManager;
  }

  @Override
  public ContainerManager getContainerManager() {
    return containerManager;
  }

  @Override
  public ReplicationManager getReplicationManager() {
    return null;
  }

  @Override
  public ContainerBalancer getContainerBalancer() {
    return null;
  }

  @Override
  public InetSocketAddress getDatanodeRpcAddress() {
    return getDatanodeProtocolServer().getDatanodeRpcAddress();
  }

  @Override
  public SCMNodeDetails getScmNodeDetails() {
    return reconNodeDetails;
  }

  @Override
  public ReconfigurationHandler getReconfigurationHandler() {
    return null;
  }

  public DBStore getScmDBStore() {
    return dbStore;
  }

  public EventQueue getEventQueue() {
    return eventQueue;
  }

  public StorageContainerServiceProvider getScmServiceProvider() {
    return scmServiceProvider;
  }

  @VisibleForTesting
  public ContainerSizeCountTask getContainerSizeCountTask() {
    return containerSizeCountTask;
  }
  @VisibleForTesting
  public ContainerCountBySizeDao getContainerCountBySizeDao() {
    return containerCountBySizeDao;
  }

  /**
   * Get SCM metadata RocksDB's latest sequence number.
   * @return latest sequence number.
   */
  private long getCurrentSCMDBSequenceNumber() {
    return scmMetadataManager.getLastSequenceNumberFromDB();
  }

  /**
   * Set the current DB store for ReconSCM.
   *
   * @param store Recon SCM DB store
   */
  @Override
  public void setStore(DBStore store) {
    this.dbStore = store;
  }

  @Override
  public DBStore getStore() {
    return this.dbStore;
  }

  private DBStore createDBAndAddSCMTablesAndCodecs(File dbFile,
                                                   ReconSCMDBDefinition definition) throws IOException {
    DBStoreBuilder dbStoreBuilder =
        DBStoreBuilder.newBuilder(ozoneConfiguration)
            .setName(dbFile.getName())
            .setPath(dbFile.toPath().getParent());
    for (DBColumnFamilyDefinition columnFamily :
        definition.getColumnFamilies()) {
      dbStoreBuilder.addTable(columnFamily.getName());
      dbStoreBuilder.addCodec(columnFamily.getKeyType(),
          columnFamily.getKeyCodec());
      dbStoreBuilder.addCodec(columnFamily.getValueType(),
          columnFamily.getValueCodec());
    }
    return dbStoreBuilder.build();
  }
}
