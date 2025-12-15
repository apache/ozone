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

package org.apache.hadoop.ozone.container.common.statemachine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.NettyMetrics;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.HddsDatanodeStopService;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.report.ReportManager;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.CloseContainerCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.ClosePipelineCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.CommandDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.CreatePipelineCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteContainerCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.FinalizeNewLayoutVersionCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.ReconcileContainerCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.ReconstructECContainersCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.RefreshVolumeUsageCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.ReplicateContainerCommandHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.SetNodeOperationalStateCommandHandler;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinator;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionMetrics;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ContainerImporter;
import org.apache.hadoop.ozone.container.replication.ContainerReplicator;
import org.apache.hadoop.ozone.container.replication.DownloadAndImportReplicator;
import org.apache.hadoop.ozone.container.replication.GrpcContainerUploader;
import org.apache.hadoop.ozone.container.replication.MeasuredReplicator;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.PushReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisorMetrics;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.apache.hadoop.ozone.container.upgrade.DataNodeUpgradeFinalizer;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State Machine Class.
 */
public class DatanodeStateMachine implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeStateMachine.class);
  private final ExecutorService executorService;
  private final ExecutorService closePipelineCommandExecutorService;
  private final ExecutorService createPipelineCommandExecutorService;
  private final ConfigurationSource conf;
  private final SCMConnectionManager connectionManager;
  private final ECReconstructionCoordinator ecReconstructionCoordinator;
  private StateContext context;
  private VolumeChoosingPolicy volumeChoosingPolicy;
  private final OzoneContainer container;
  private final DatanodeDetails datanodeDetails;
  private final CommandDispatcher commandDispatcher;
  private final ReportManager reportManager;
  private final AtomicLong nextHB;
  private volatile Thread stateMachineThread = null;
  private Thread cmdProcessThread = null;
  private final ReplicationSupervisor supervisor;

  private final HddsDatanodeStopService hddsDatanodeStopService;

  private final HDDSLayoutVersionManager layoutVersionManager;
  private final DatanodeLayoutStorage layoutStorage;
  private final DataNodeUpgradeFinalizer upgradeFinalizer;

  /**
   * Used to synchronize to the OzoneContainer object created in the
   * constructor in a non-thread-safe way - see HDDS-3116.
   */
  private final ReadWriteLock constructionLock = new ReentrantReadWriteLock();
  private final MeasuredReplicator pullReplicatorWithMetrics;
  private final MeasuredReplicator pushReplicatorWithMetrics;
  private final ReplicationSupervisorMetrics replicationSupervisorMetrics;
  private final NettyMetrics nettyMetrics;
  private final ECReconstructionMetrics ecReconstructionMetrics;
  // This is an instance variable as mockito needs to access it in a test
  @SuppressWarnings("FieldCanBeLocal")
  private final ReconstructECContainersCommandHandler
      reconstructECContainersCommandHandler;

  private final DatanodeQueueMetrics queueMetrics;
  private final ReconfigurationHandler reconfigurationHandler;

  /**
   * Constructs a datanode state machine.
   * @param datanodeDetails - DatanodeDetails used to identify a datanode
   * @param conf - Configuration.
   * @param certClient - Datanode Certificate client, required if security is
   *                     enabled
   */
  @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:methodlength"})
  public DatanodeStateMachine(HddsDatanodeService hddsDatanodeService,
                              DatanodeDetails datanodeDetails,
                              ConfigurationSource conf,
                              CertificateClient certClient,
                              SecretKeyClient secretKeyClient,
                              HddsDatanodeStopService hddsDatanodeStopService,
                              ReconfigurationHandler reconfigurationHandler)
      throws IOException {
    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);

    this.reconfigurationHandler = reconfigurationHandler;
    this.hddsDatanodeStopService = hddsDatanodeStopService;
    this.conf = conf;
    this.datanodeDetails = datanodeDetails;

    Clock clock = Clock.system(ZoneId.systemDefault());
    // Expected to be initialized already.
    layoutStorage = new DatanodeLayoutStorage(conf,
        datanodeDetails.getUuidString());

    layoutVersionManager = new HDDSLayoutVersionManager(
        layoutStorage.getLayoutVersion());
    upgradeFinalizer = new DataNodeUpgradeFinalizer(layoutVersionManager);
    VersionedDatanodeFeatures.initialize(layoutVersionManager);

    String threadNamePrefix = datanodeDetails.threadNamePrefix();
    executorService = Executors.newFixedThreadPool(
        getEndPointTaskThreadPoolSize(),
        new ThreadFactoryBuilder()
            .setNameFormat(threadNamePrefix +
                "DatanodeStateMachineTaskThread-%d")
            .build());
    connectionManager = new SCMConnectionManager(conf);
    context = new StateContext(this.conf, DatanodeStates.getInitState(), this,
        threadNamePrefix);
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
    // OzoneContainer instance is used in a non-thread safe way by the context
    // past to its constructor, so we much synchronize its access. See
    // HDDS-3116 for more details.
    constructionLock.writeLock().lock();
    try {
      container = new OzoneContainer(hddsDatanodeService, this.datanodeDetails,
          conf, context, certClient, secretKeyClient, volumeChoosingPolicy);
    } finally {
      constructionLock.writeLock().unlock();
    }
    nextHB = new AtomicLong(Time.monotonicNow());

    ContainerImporter importer = new ContainerImporter(conf,
        container.getContainerSet(),
        container.getController(),
        container.getVolumeSet(),
        volumeChoosingPolicy);
    ContainerReplicator pullReplicator = new DownloadAndImportReplicator(
        conf, container.getContainerSet(),
        importer,
        new SimpleContainerDownloader(conf, certClient));
    ContainerReplicator pushReplicator = new PushReplicator(conf,
        new OnDemandContainerReplicationSource(container.getController()),
        new GrpcContainerUploader(conf, certClient, container.getController())
    );

    pullReplicatorWithMetrics = new MeasuredReplicator(pullReplicator, "pull");
    pushReplicatorWithMetrics = new MeasuredReplicator(pushReplicator, "push");

    ReplicationConfig replicationConfig =
        conf.getObject(ReplicationConfig.class);
    supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .datanodeConfig(dnConf)
        .replicationConfig(replicationConfig)
        .clock(clock)
        .build();

    replicationSupervisorMetrics =
        ReplicationSupervisorMetrics.create(supervisor);

    ecReconstructionMetrics = ECReconstructionMetrics.create();
    ecReconstructionCoordinator = new ECReconstructionCoordinator(
        conf, certClient, secretKeyClient, context, ecReconstructionMetrics,
        threadNamePrefix);

    // This is created as an instance variable as Mockito needs to access it in
    // a test. The test mocks it in a running mini-cluster.
    reconstructECContainersCommandHandler =
        new ReconstructECContainersCommandHandler(conf, supervisor,
            ecReconstructionCoordinator);

    // TODO HDDS-11218 combine the clients used for reconstruction and reconciliation so they share the same cache of
    //  datanode clients.
    DNContainerOperationClient dnClient = new DNContainerOperationClient(conf, certClient, secretKeyClient);

    // Create separate bounded executors for pipeline command handlers
    ThreadFactory closePipelineThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(threadNamePrefix + "ClosePipelineCommandHandlerThread-%d")
        .build();
    closePipelineCommandExecutorService = new ThreadPoolExecutor(
        1, 1,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(dnConf.getCommandQueueLimit()),
        closePipelineThreadFactory);

    ThreadFactory createPipelineThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(threadNamePrefix + "CreatePipelineCommandHandlerThread-%d")
        .build();
    createPipelineCommandExecutorService = new ThreadPoolExecutor(
        1, 1,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(dnConf.getCommandQueueLimit()),
        createPipelineThreadFactory);

    // When we add new handlers just adding a new handler here should do the
    // trick.
    CommandDispatcher.Builder dispatcherBuilder = CommandDispatcher.newBuilder()
        .addHandler(new CloseContainerCommandHandler(
            dnConf.getContainerCloseThreads(),
            dnConf.getCommandQueueLimit(), threadNamePrefix))
        .addHandler(new DeleteBlocksCommandHandler(getContainer(),
            conf, dnConf, threadNamePrefix))
        .addHandler(new ReplicateContainerCommandHandler(conf, supervisor,
            pullReplicatorWithMetrics, pushReplicatorWithMetrics))
        .addHandler(reconstructECContainersCommandHandler)
        .addHandler(new DeleteContainerCommandHandler(
            dnConf.getContainerDeleteThreads(), clock,
            dnConf.getCommandQueueLimit(), threadNamePrefix))
        .addHandler(new ClosePipelineCommandHandler(conf,
            closePipelineCommandExecutorService))
        .addHandler(new CreatePipelineCommandHandler(conf,
            createPipelineCommandExecutorService))
        .addHandler(new FinalizeNewLayoutVersionCommandHandler())
        .addHandler(new RefreshVolumeUsageCommandHandler())
        .addHandler(new ReconcileContainerCommandHandler(supervisor, dnClient));

    if (container.getDiskBalancerService() != null) {
      dispatcherBuilder.addHandler(new SetNodeOperationalStateCommandHandler(
          conf, supervisor::nodeStateUpdated,
          container.getDiskBalancerService()::nodeStateUpdated));
    } else {
      dispatcherBuilder.addHandler(new SetNodeOperationalStateCommandHandler(
          conf, supervisor::nodeStateUpdated, null));
    }

    dispatcherBuilder
        .setConnectionManager(connectionManager)
        .setContainer(container)
        .setContext(context);

    commandDispatcher = dispatcherBuilder.build();

    reportManager = ReportManager.newBuilder(conf)
        .setStateContext(context)
        .addPublisherFor(NodeReportProto.class)
        .addPublisherFor(ContainerReportsProto.class)
        .addPublisherFor(CommandStatusReportsProto.class)
        .addPublisherFor(PipelineReportsProto.class)
        .addThreadNamePrefix(threadNamePrefix)
        .build();

    queueMetrics = DatanodeQueueMetrics.create(this);
    nettyMetrics = NettyMetrics.create();
  }

  @VisibleForTesting
  public DatanodeStateMachine(DatanodeDetails datanodeDetails,
                              ConfigurationSource conf) throws IOException {
    this(null, datanodeDetails, conf, null, null, null,
        new ReconfigurationHandler("DN", (OzoneConfiguration) conf, op -> { }));
  }

  private int getEndPointTaskThreadPoolSize() {
    // TODO(runzhiwang): The default totalServerCount here is set to 1,
    //  which requires additional processing if want to increase
    //  the number of recons.
    int totalServerCount = 1;

    try {
      totalServerCount += HddsUtils.getSCMAddressForDatanodes(conf).size();
    } catch (Exception e) {
      LOG.error("Fail to get scm addresses", e);
    }

    LOG.info("Datanode State Machine Task Thread Pool size {}",
        totalServerCount);
    return totalServerCount;
  }

  /**
   *
   * Return DatanodeDetails if set, return null otherwise.
   *
   * @return DatanodeDetails
   */
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  /**
   * Returns the Connection manager for this state machine.
   *
   * @return - SCMConnectionManager.
   */
  public SCMConnectionManager getConnectionManager() {
    return connectionManager;
  }

  public OzoneContainer getContainer() {
    // See HDDS-3116 to explain the need for this lock
    constructionLock.readLock().lock();
    try {
      return this.container;
    } finally {
      constructionLock.readLock().unlock();
    }
  }

  /**
   * Runs the state machine at a fixed frequency.
   */
  private void startStateMachineThread() throws IOException {
    reportManager.init();
    initCommandHandlerThread(conf);

    upgradeFinalizer.runPrefinalizeStateActions(layoutStorage, this);
    LOG.info("Ozone container server started.");
    while (context.getState() != DatanodeStates.SHUTDOWN) {
      try {
        LOG.debug("Executing cycle Number : {}", context.getExecutionCount());
        long heartbeatFrequency = context.getHeartbeatFrequency();
        nextHB.set(Time.monotonicNow() + heartbeatFrequency);
        context.execute(executorService, heartbeatFrequency,
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // Someone has sent interrupt signal, this could be because
        // 1. Trigger heartbeat immediately
        // 2. Shutdown has be initiated.
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error("Unable to finish the execution.", e);
      }

      long now = Time.monotonicNow();
      if (now < nextHB.get()) {
        if (!Thread.interrupted()) {
          try {
            Thread.sleep(nextHB.get() - now);
          } catch (InterruptedException e) {
            // TriggerHeartbeat is called during the sleep. Don't need to set
            // the interrupted state to true.
          }
        }
      }
    }

    // If we have got some exception in stateMachine we set the state to
    // shutdown to stop the stateMachine thread. Along with this we should
    // also stop the datanode.
    if (context.getShutdownOnError()) {
      LOG.error("DatanodeStateMachine Shutdown due to an critical error");
      hddsDatanodeStopService.stopService();
    }
  }

  public void handleFatalVolumeFailures() {
    LOG.error("DatanodeStateMachine Shutdown due to too many bad volumes, "
        + "check " + DatanodeConfiguration.FAILED_DATA_VOLUMES_TOLERATED_KEY
        + " and " + DatanodeConfiguration.FAILED_METADATA_VOLUMES_TOLERATED_KEY
        + " and " + DatanodeConfiguration.FAILED_DB_VOLUMES_TOLERATED_KEY);
    hddsDatanodeStopService.stopService();
  }

  /**
   * Gets the current context.
   *
   * @return StateContext
   */
  public StateContext getContext() {
    return context;
  }

  /**
   * Sets the current context.
   *
   * @param context - Context
   */
  public void setContext(StateContext context) {
    this.context = context;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful attention. It is strongly advised to relinquish the
   * underlying resources and to internally <em>mark</em> the {@code Closeable}
   * as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    IOUtils.close(LOG, ecReconstructionCoordinator);
    if (stateMachineThread != null) {
      stateMachineThread.interrupt();
    }
    if (cmdProcessThread != null) {
      cmdProcessThread.interrupt();
    }
    if (layoutVersionManager != null) {
      layoutVersionManager.close();
    }
    context.setState(DatanodeStates.getLastState());
    replicationSupervisorMetrics.unRegister();
    ecReconstructionMetrics.unRegister();
    executorServiceShutdownGraceful(executorService);
    executorServiceShutdownGraceful(closePipelineCommandExecutorService);
    executorServiceShutdownGraceful(createPipelineCommandExecutorService);

    if (connectionManager != null) {
      connectionManager.close();
    }

    if (container != null) {
      container.stop();
    }

    if (commandDispatcher != null) {
      commandDispatcher.stop();
    }

    if (queueMetrics != null) {
      DatanodeQueueMetrics.unRegister();
    }

    if (nettyMetrics != null) {
      nettyMetrics.unregister();
    }
  }

  private void executorServiceShutdownGraceful(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }

      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown state machine properly.");
      }
    } catch (InterruptedException e) {
      LOG.error("Error attempting to shutdown.", e);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * States that a datanode  can be in. GetNextState will move this enum from
   * getInitState to getLastState.
   */
  public enum DatanodeStates {
    INIT(1),
    RUNNING(2),
    SHUTDOWN(3);
    private final int value;

    /**
     * Constructs states.
     *
     * @param value  Enum Value
     */
    DatanodeStates(int value) {
      this.value = value;
    }

    /**
     * Returns the first State.
     *
     * @return First State.
     */
    public static DatanodeStates getInitState() {
      return INIT;
    }

    /**
     * The last state of endpoint states.
     *
     * @return last state.
     */
    public static DatanodeStates getLastState() {
      return SHUTDOWN;
    }

    /**
     * returns the numeric value associated with the endPoint.
     *
     * @return int.
     */
    public int getValue() {
      return value;
    }

    /**
     * Returns the next logical state that endPoint should move to. This
     * function assumes the States are sequentially numbered.
     *
     * @return NextState.
     */
    public DatanodeStates getNextState() {
      if (this.value < getLastState().getValue()) {
        int stateValue = this.getValue() + 1;
        for (DatanodeStates iter : values()) {
          if (stateValue == iter.getValue()) {
            return iter;
          }
        }
      }
      return getLastState();
    }

    public boolean isTransitionAllowedTo(DatanodeStates newState) {
      return newState.getValue() > getValue();
    }
  }

  /**
   * Start datanode state machine as a single thread daemon.
   */
  public void startDaemon() {
    Runnable startStateMachineTask = () -> {
      try {
        startStateMachineThread();
      } catch (Exception ex) {
        LOG.error("Unable to start the DatanodeState Machine", ex);
      }
    };
    stateMachineThread =  new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(datanodeDetails.threadNamePrefix() +
            "DatanodeStateMachineDaemonThread")
        .setUncaughtExceptionHandler((Thread t, Throwable ex) -> {
          String message = "Terminate Datanode, encounter uncaught exception"
              + " in Datanode State Machine Thread";
          ExitUtils.terminate(1, message, ex, LOG);
        })
        .build().newThread(startStateMachineTask);
    stateMachineThread.setPriority(Thread.MAX_PRIORITY);
    stateMachineThread.start();
  }

  /**
   * Calling this will immediately trigger a heartbeat to the SCMs.
   * This heartbeat will also include all the reports which are ready to
   * be sent by datanode.
   */
  public void triggerHeartbeat() {
    if (stateMachineThread != null && isDaemonStarted()) {
      stateMachineThread.interrupt();
    }
  }

  /**
   * Waits for DatanodeStateMachine to exit.
   */
  public void join() throws InterruptedException {
    if (stateMachineThread != null) {
      stateMachineThread.join();
    }

    if (cmdProcessThread != null) {
      cmdProcessThread.join();
    }
  }

  /**
   * Returns a summary of the commands queued in the datanode. Commands are
   * queued in two places. In the CommandQueue inside the StateContext object.
   * A single thread picks commands from there and hands the command to the
   * CommandDispatcher. This finds the handler for the command based on its
   * command type and either executes the command immediately in the current
   * (single) thread, or queues it in the handler where a thread pool executor
   * will process it. The total commands queued in the datanode is therefore
   * the sum those in the CommandQueue and the dispatcher queues.
   * @return EnumCounters containing a count for each known command.
   */
  public EnumCounters<SCMCommandProto.Type> getQueuedCommandCount() {
    // Get command counts from StateContext command queue
    EnumCounters<SCMCommandProto.Type> commandQSummary =
        context.getCommandQueueSummary();
    // This EnumCounters will contain an entry for every command type which is registered
    // with the dispatcher, and that should be all command types the DN knows
    // about. Any commands with nothing in the queue will have a count of
    // zero.
    EnumCounters<SCMCommandProto.Type> dispatcherQSummary =
        commandDispatcher.getQueuedCommandCount();
    // Merge the two EnumCounters into the fully populated one having a count
    // for all known command types.
    dispatcherQSummary.add(commandQSummary);
    return dispatcherQSummary;
  }

  /**
   * Stop the daemon thread of the datanode state machine.
   */
  public synchronized void stopDaemon() {
    try {
      IOUtils.close(LOG, pushReplicatorWithMetrics, pullReplicatorWithMetrics);
      supervisor.stop();
      context.setShutdownGracefully();
      context.setState(DatanodeStates.SHUTDOWN);
      reportManager.shutdown();
      this.close();
      LOG.info("Ozone container server stopped.");
    } catch (IOException e) {
      LOG.error("Stop ozone container server failed.", e);
    }
  }

  public boolean isDaemonStarted() {
    return this.getContext().getExecutionCount() > 0;
  }

  /**
   *
   * Check if the datanode state machine daemon is stopped.
   *
   * @return True if datanode state machine daemon is stopped
   * and false otherwise.
   */
  @VisibleForTesting
  public boolean isDaemonStopped() {
    return this.executorService.isShutdown()
        && this.getContext().getState() == DatanodeStates.SHUTDOWN;
  }

  /**
   * Create a command handler thread.
   */
  private void initCommandHandlerThread(ConfigurationSource config) {

    /*
     * Task that periodically checks if we have any outstanding commands.
     * It is assumed that commands can be processed slowly and in order.
     * This assumption might change in future. Right now due to this assumption
     * we have single command  queue process thread.
     */
    Runnable processCommandQueue = () -> {
      while (getContext().getState() != DatanodeStates.SHUTDOWN) {
        SCMCommand<?> command = getContext().getNextCommand();
        if (command != null) {
          commandDispatcher.handle(command);
        } else {
          try {
            // Sleep till the next HB + 1 second.
            long now = Time.monotonicNow();
            if (nextHB.get() > now) {
              Thread.sleep((nextHB.get() - now) + 1000L);
            }
          } catch (InterruptedException e) {
            // Ignore this exception.
            Thread.currentThread().interrupt();
          }
        }
      }
    };

    // We will have only one thread for command processing in a datanode.
    cmdProcessThread = getCommandHandlerThread(processCommandQueue);
    cmdProcessThread.setPriority(Thread.NORM_PRIORITY);
    cmdProcessThread.start();
  }

  private Thread getCommandHandlerThread(Runnable processCommandQueue) {
    Thread handlerThread = new Thread(processCommandQueue);
    handlerThread.setDaemon(true);
    handlerThread.setName(
        datanodeDetails.threadNamePrefix() + "CommandProcessorThread");
    handlerThread.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
      // Let us just restart this thread after logging a critical error.
      // if this thread is not running we cannot handle commands from SCM.
      LOG.error("Critical Error : Command processor thread encountered an " +
          "error. Thread: {}", t.toString(), e);
      getCommandHandlerThread(processCommandQueue).start();
    });
    return handlerThread;
  }

  /**
   * returns the Command Dispatcher.
   * @return CommandDispatcher
   */
  @VisibleForTesting
  public CommandDispatcher getCommandDispatcher() {
    return commandDispatcher;
  }

  @VisibleForTesting
  public ReplicationSupervisor getSupervisor() {
    return supervisor;
  }

  @VisibleForTesting
  public HDDSLayoutVersionManager getLayoutVersionManager() {
    return layoutVersionManager;
  }

  @VisibleForTesting
  public DatanodeLayoutStorage getLayoutStorage() {
    return layoutStorage;
  }

  public StatusAndMessages finalizeUpgrade()
      throws IOException {
    return upgradeFinalizer.finalize(datanodeDetails.getUuidString(), this);
  }

  public StatusAndMessages queryUpgradeStatus()
      throws IOException {
    return upgradeFinalizer.reportStatus(datanodeDetails.getUuidString(),
        true);
  }

  public UpgradeFinalizer<DatanodeStateMachine> getUpgradeFinalizer() {
    return upgradeFinalizer;
  }

  public ConfigurationSource getConf() {
    return conf;
  }

  public DatanodeQueueMetrics getQueueMetrics() {
    return queueMetrics;
  }

  public ReconfigurationHandler getReconfigurationHandler() {
    return reconfigurationHandler;
  }

  public Thread getStateMachineThread() {
    return stateMachineThread;
  }

  public Thread getCmdProcessThread() {
    return cmdProcessThread;
  }

  public VolumeChoosingPolicy getVolumeChoosingPolicy() {
    return volumeChoosingPolicy;
  }

  /**
   * Sets the next heartbeat time. Setting to current time will trigger HB immediately as will be less than time
   * compared to Time.monotonicNow() when compared.
   * @param time
   */
  public void setNextHB(long time) {
    nextHB.set(time);
  }

  @VisibleForTesting
  public ExecutorService getExecutorService() {
    return executorService;
  }

  /**
   * Resize the executor based on the number of active endpoint tasks.
   */
  public void resizeExecutor(int size) {
    if (executorService instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor tpe = (ThreadPoolExecutor) executorService;
      HddsServerUtil.setPoolSize(tpe, size, LOG);
    }
  }
}
