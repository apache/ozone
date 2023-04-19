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
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineActionHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.fsck.ContainerHealthTask;
import org.apache.hadoop.ozone.recon.fsck.ReconSafeModeMgrTask;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import com.google.inject.Inject;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.RECON_SCM_CONFIG_PREFIX;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.buildRpcServerStartMessage;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY;

import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReport;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;

import org.apache.ratis.util.ExitUtils;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's 'lite' version of SCM.
 */
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

  private ScheduledExecutorService scheduler;

  private AtomicBoolean isSyncDataFromSCMRunning;

  @Inject
  public ReconStorageContainerManagerFacade(OzoneConfiguration conf,
      StorageContainerServiceProvider scmServiceProvider,
      ReconTaskStatusDao reconTaskStatusDao,
      ContainerHealthSchemaManager containerHealthSchemaManager,
      ReconContainerMetadataManager reconContainerMetadataManager,
      ReconUtils reconUtils,
      ReconSafeModeManager safeModeManager) throws IOException {
    reconNodeDetails = getReconNodeDetails(conf);
    this.eventQueue = new EventQueue();
    eventQueue.setSilent(true);
    this.scmContext = new SCMContext.Builder()
        .setIsPreCheckComplete(true)
        .setSCM(this)
        .build();
    this.ozoneConfiguration = getReconScmConfiguration(conf);
    this.scmStorageConfig = new ReconStorageConfig(conf, reconUtils);
    this.clusterMap = new NetworkTopologyImpl(conf);
    this.dbStore = DBStoreBuilder
        .createDBStore(ozoneConfiguration, new ReconSCMDBDefinition());

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
        eventQueue, scmhaManager, scmContext);
    ContainerReplicaPendingOps pendingOps = new ContainerReplicaPendingOps(
        conf, Clock.system(ZoneId.systemDefault()));
    this.containerManager = new ReconContainerManager(conf,
        dbStore, ReconSCMDBDefinition.CONTAINERS.getTable(dbStore),
        pipelineManager, scmServiceProvider,
        containerHealthSchemaManager, reconContainerMetadataManager,
        scmhaManager, sequenceIdGen, pendingOps);
    this.scmServiceProvider = scmServiceProvider;
    this.isSyncDataFromSCMRunning = new AtomicBoolean();

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
        pipelineManager, nodeManager, scmServiceProvider,
        reconTaskStatusDao, reconTaskConfig);
    ContainerHealthTask containerHealthTask = new ContainerHealthTask(
        containerManager, scmServiceProvider,
        reconTaskStatusDao, containerHealthSchemaManager,
        containerPlacementPolicy, reconTaskConfig);

    StaleNodeHandler staleNodeHandler =
        new ReconStaleNodeHandler(nodeManager, pipelineManager,
            conf, pipelineSyncTask);
    DeadNodeHandler deadNodeHandler = new ReconDeadNodeHandler(nodeManager,
        pipelineManager, containerManager,
        scmServiceProvider, containerHealthTask, pipelineSyncTask);

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
        = ScmUtils.initContainerReportQueue(ozoneConfiguration);
    List<ThreadPoolExecutor> executors
        = FixedThreadPoolWithAffinityExecutor.initializeExecutorPool(queues);
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
    reconSafeModeMgrTask = new ReconSafeModeMgrTask(
        containerManager, nodeManager, safeModeManager,
        reconTaskConfig, ozoneConfiguration);
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

  private SCMNodeDetails getReconNodeDetails(OzoneConfiguration conf) {
    SCMNodeDetails.Builder builder = new SCMNodeDetails.Builder();
    builder.setDatanodeProtocolServerAddress(
        HddsServerUtil.getReconDataNodeBindAddress(conf));
    return builder.build();
  }

  /**
   * Start the Recon SCM subsystems.
   */
  @Override
  public void start() {
    if (LOG.isInfoEnabled()) {
      LOG.info(buildRpcServerStartMessage(
          "Recon ScmDatanodeProtocol RPC server",
          getDatanodeProtocolServer().getDatanodeRpcAddress()));
    }
    scheduler = Executors.newScheduledThreadPool(1);
    boolean isSCMSnapshotEnabled = ozoneConfiguration.getBoolean(
        ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_ENABLED,
        ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_ENABLED_DEFAULT);
    if (isSCMSnapshotEnabled) {
      initializeSCMDB();
      LOG.info("SCM DB initialized");
    } else {
      initializePipelinesFromScm();
    }
    LOG.debug("Started the SCM Container Info sync scheduler.");
    long interval = ozoneConfiguration.getTimeDuration(
        OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY,
        OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    long initialDelay = ozoneConfiguration.getTimeDuration(
        OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY,
        OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT,
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
    IOUtils.cleanupWithLogger(LOG, nodeManager);
    IOUtils.cleanupWithLogger(LOG, containerManager);
    IOUtils.cleanupWithLogger(LOG, pipelineManager);
    LOG.info("Flushing container replica history to DB.");
    containerManager.flushReplicaHistoryMapToDB(true);
    IOUtils.close(LOG, dbStore);
  }

  @Override
  public void shutDown(String message) {
    stop();
    ExitUtils.terminate(0, message, LOG);
  }

  public ReconDatanodeProtocolServer getDatanodeProtocolServer() {
    return datanodeProtocolServer;
  }

  private void initializePipelinesFromScm() {
    try {
      List<Pipeline> pipelinesFromScm = scmServiceProvider.getPipelines();
      LOG.info("Obtained {} pipelines from SCM.", pipelinesFromScm.size());
      pipelineManager.initializePipelines(pipelinesFromScm);
    } catch (IOException | TimeoutException ioEx) {
      LOG.error("Exception encountered while getting pipelines from SCM.",
          ioEx);
    }
  }

  private void initializeSCMDB() {
    try {
      long scmContainersCount = scmServiceProvider.getContainerCount();
      long reconContainerCount = containerManager.getContainers().size();
      long threshold = ozoneConfiguration.getInt(
          ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD,
          ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT);

      if (Math.abs(scmContainersCount - reconContainerCount) > threshold) {
        LOG.info("Recon Container Count: {}, SCM Container Count: {}",
            reconContainerCount, scmContainersCount);
        updateReconSCMDBWithNewSnapshot();
        LOG.info("Updated Recon DB with SCM DB");
      } else {
        initializePipelinesFromScm();
      }
    } catch (IOException e) {
      LOG.error("Exception encountered while getting SCM DB.");
    } finally {
      isSyncDataFromSCMRunning.compareAndSet(true, false);
    }
  }

  public void updateReconSCMDBWithNewSnapshot() throws IOException {
    if (isSyncDataFromSCMRunning.compareAndSet(false, true)) {
      DBCheckpoint dbSnapshot = scmServiceProvider.getSCMDBSnapshot();
      if (dbSnapshot != null && dbSnapshot.getCheckpointLocation() != null) {
        LOG.info("Got new checkpoint from SCM : " +
            dbSnapshot.getCheckpointLocation());
        try {
          initializeNewRdbStore(dbSnapshot.getCheckpointLocation().toFile());
        } catch (IOException e) {
          LOG.error("Unable to refresh Recon SCM DB Snapshot. ", e);
        }
      } else {
        LOG.error("Null snapshot location got from SCM.");
      }
    } else {
      LOG.warn("SCM DB sync is already running.");
    }
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
                  } catch (TimeoutException e) {
                    LOG.error("Could not add new container {} in Recon " +
                        "container manager cache.", containerID);
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

  private void deleteOldSCMDB() throws IOException {
    if (dbStore != null) {
      File oldDBLocation = dbStore.getDbLocation();
      if (oldDBLocation.exists()) {
        LOG.info("Cleaning up old SCM snapshot db at {}.",
            oldDBLocation.getAbsolutePath());
        FileUtils.deleteDirectory(oldDBLocation);
      }
    }
  }

  private void initializeNewRdbStore(File dbFile) throws IOException {
    try {
      DBStore newStore = createDBAndAddSCMTablesAndCodecs(
          dbFile, new ReconSCMDBDefinition());
      Table<UUID, DatanodeDetails> nodeTable =
          ReconSCMDBDefinition.NODES.getTable(dbStore);
      Table<UUID, DatanodeDetails> newNodeTable =
          ReconSCMDBDefinition.NODES.getTable(newStore);
      try (TableIterator<UUID, ? extends KeyValue<UUID,
          DatanodeDetails>> iterator = nodeTable.iterator()) {
        while (iterator.hasNext()) {
          KeyValue<UUID, DatanodeDetails> keyValue = iterator.next();
          newNodeTable.put(keyValue.getKey(), keyValue.getValue());
        }
      }
      sequenceIdGen.reinitialize(
          ReconSCMDBDefinition.SEQUENCE_ID.getTable(newStore));
      pipelineManager.reinitialize(
          ReconSCMDBDefinition.PIPELINES.getTable(newStore));
      containerManager.reinitialize(
          ReconSCMDBDefinition.CONTAINERS.getTable(newStore));
      nodeManager.reinitialize(
          ReconSCMDBDefinition.NODES.getTable(newStore));
      IOUtils.close(LOG, dbStore);
      deleteOldSCMDB();
      dbStore = newStore;
      File newDb = new File(dbFile.getParent() +
          OZONE_URI_DELIMITER + ReconSCMDBDefinition.RECON_SCM_DB_NAME);
      boolean success = dbFile.renameTo(newDb);
      if (success) {
        LOG.info("SCM snapshot linked to Recon DB.");
      }
      LOG.info("Created SCM DB handle from snapshot at {}.",
          dbFile.getAbsolutePath());
    } catch (IOException ioEx) {
      LOG.error("Unable to initialize Recon SCM DB snapshot store.", ioEx);
    }
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

  public EventQueue getEventQueue() {
    return eventQueue;
  }

  public StorageContainerServiceProvider getScmServiceProvider() {
    return scmServiceProvider;
  }
}
