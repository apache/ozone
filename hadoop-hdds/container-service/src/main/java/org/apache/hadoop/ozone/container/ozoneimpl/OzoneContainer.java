/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.BlockDeletingService;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.utils.ContainerInspectorUtil;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume.VolumeType;
import org.apache.hadoop.ozone.container.common.volume.StorageVolumeChecker;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.StaleRecoveringContainerScrubbingService;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStore;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStoreImpl;
import org.apache.hadoop.ozone.container.replication.ContainerImporter;
import org.apache.hadoop.ozone.container.replication.MeasuredReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationServer;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures.SchemaV3;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.Timer;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_WORKERS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_WORKERS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_WORKERS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_WORKERS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.ON_DEMAND_VOLUME_BYTES_PER_SECOND_KEY;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.VOLUME_BYTES_PER_SECOND_KEY;

/**
 * Ozone main class sets up the network servers and initializes the container
 * layer.
 */
public class OzoneContainer {

  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneContainer.class);

  private final HddsDispatcher hddsDispatcher;
  private final Map<ContainerType, Handler> handlers;
  private final ConfigurationSource config;
  private final MutableVolumeSet volumeSet;
  private final MutableVolumeSet metaVolumeSet;
  private final MutableVolumeSet dbVolumeSet;
  private final StorageVolumeChecker volumeChecker;
  private final ContainerSet containerSet;
  private final XceiverServerSpi writeChannel;
  private final XceiverServerSpi readChannel;
  private final ContainerController controller;
  private BackgroundContainerMetadataScanner metadataScanner;
  private List<BackgroundContainerDataScanner> dataScanners;
  private List<AbstractBackgroundContainerScanner> backgroundScanners;
  private final BlockDeletingService blockDeletingService;
  private final StaleRecoveringContainerScrubbingService
      recoveringContainerScrubbingService;
  private final GrpcTlsConfig tlsClientConfig;
  private final AtomicReference<InitializingStatus> initializingStatus;
  private final ReplicationServer replicationServer;
  private DatanodeDetails datanodeDetails;
  private StateContext context;
  private ScheduledExecutorService dbCompactionExecutorService;

  private final ContainerMetrics metrics;
  private WitnessedContainerMetadataStore witnessedContainerMetadataStore;

  private HddsDatanodeService hddsDatanodeService;

  enum InitializingStatus {
    UNINITIALIZED, INITIALIZING, INITIALIZED
  }

  /**
   * Construct OzoneContainer object.
   *
   * @param datanodeDetails
   * @param conf
   * @param certClient
   * @throws DiskOutOfSpaceException
   * @throws IOException
   */
  public OzoneContainer(HddsDatanodeService hddsDatanodeService,
      DatanodeDetails datanodeDetails, ConfigurationSource conf,
      StateContext context, CertificateClient certClient,
      SecretKeyVerifierClient secretKeyClient) throws IOException {
    config = conf;
    this.datanodeDetails = datanodeDetails;
    this.context = context;
    this.volumeChecker = new StorageVolumeChecker(conf, new Timer(),
        datanodeDetails.threadNamePrefix());

    volumeSet = new MutableVolumeSet(datanodeDetails.getUuidString(), conf,
        context, VolumeType.DATA_VOLUME, volumeChecker);
    volumeSet.setFailedVolumeListener(this::handleVolumeFailures);
    metaVolumeSet = new MutableVolumeSet(datanodeDetails.getUuidString(), conf,
        context, VolumeType.META_VOLUME, volumeChecker);

    dbVolumeSet = HddsServerUtil.getDatanodeDbDirs(conf).isEmpty() ? null :
        new MutableVolumeSet(datanodeDetails.getUuidString(), conf,
            context, VolumeType.DB_VOLUME, volumeChecker);
    final DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    if (SchemaV3.isFinalizedAndEnabled(config)) {
      HddsVolumeUtil.loadAllHddsVolumeDbStore(
          volumeSet, dbVolumeSet, false, LOG);
      if (dnConf.autoCompactionSmallSstFile()) {
        this.dbCompactionExecutorService = Executors.newScheduledThreadPool(
                dnConf.getAutoCompactionSmallSstFileThreads(),
            new ThreadFactoryBuilder().setNameFormat(
                datanodeDetails.threadNamePrefix() +
                    "RocksDBCompactionThread-%d").build());
        this.dbCompactionExecutorService.scheduleWithFixedDelay(this::compactDb,
            dnConf.getAutoCompactionSmallSstFileIntervalMinutes(),
            dnConf.getAutoCompactionSmallSstFileIntervalMinutes(),
            TimeUnit.MINUTES);
      }
    }
    long recoveringContainerTimeout = config.getTimeDuration(
        OZONE_RECOVERING_CONTAINER_TIMEOUT,
        OZONE_RECOVERING_CONTAINER_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    this.witnessedContainerMetadataStore = WitnessedContainerMetadataStoreImpl.get(conf);
    containerSet = new ContainerSet(witnessedContainerMetadataStore.getContainerIdsTable(), recoveringContainerTimeout);
    metadataScanner = null;

    metrics = ContainerMetrics.create(conf);
    handlers = Maps.newHashMap();

    IncrementalReportSender<Container> icrSender = container -> {
      synchronized (containerSet) {
        ContainerReplicaProto containerReport = container.getContainerReport();

        IncrementalContainerReportProto icr = IncrementalContainerReportProto
            .newBuilder()
            .addReport(containerReport)
            .build();
        context.addIncrementalReport(icr);
        context.getParent().triggerHeartbeat();
      }
    };

    for (ContainerType containerType : ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(
              containerType, conf,
              context.getParent().getDatanodeDetails().getUuidString(),
              containerSet, volumeSet, metrics, icrSender));
    }

    SecurityConfig secConf = new SecurityConfig(conf);
    hddsDispatcher = new HddsDispatcher(config, containerSet, volumeSet,
        handlers, context, metrics,
        TokenVerifier.create(secConf, secretKeyClient));

    /*
     * ContainerController is the control plane
     * XceiverServerRatis is the write channel
     * XceiverServerGrpc is the read channel
     */
    controller = new ContainerController(containerSet, handlers);

    writeChannel = XceiverServerRatis.newXceiverServerRatis(hddsDatanodeService,
        datanodeDetails, config, hddsDispatcher, controller, certClient,
        context);

    replicationServer = new ReplicationServer(
        controller,
        conf.getObject(ReplicationConfig.class),
        secConf,
        certClient,
        new ContainerImporter(conf, containerSet, controller,
            volumeSet),
        datanodeDetails.threadNamePrefix());

    readChannel = new XceiverServerGrpc(
        datanodeDetails, config, hddsDispatcher, certClient);
    Duration blockDeletingSvcInterval = dnConf.getBlockDeletionInterval();

    long blockDeletingServiceTimeout = config
        .getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);

    int blockDeletingServiceWorkerSize = config
        .getInt(OZONE_BLOCK_DELETING_SERVICE_WORKERS,
            OZONE_BLOCK_DELETING_SERVICE_WORKERS_DEFAULT);
    blockDeletingService =
        new BlockDeletingService(this, blockDeletingSvcInterval.toMillis(),
            blockDeletingServiceTimeout, TimeUnit.MILLISECONDS,
            blockDeletingServiceWorkerSize, config,
            datanodeDetails.threadNamePrefix(),
            context.getParent().getReconfigurationHandler());

    Duration recoveringContainerScrubbingSvcInterval =
        dnConf.getRecoveringContainerScrubInterval();

    long recoveringContainerScrubbingServiceTimeout = config
        .getTimeDuration(OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_TIMEOUT,
            OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);

    int recoveringContainerScrubbingServiceWorkerSize = config
        .getInt(OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_WORKERS,
            OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_WORKERS_DEFAULT);

    recoveringContainerScrubbingService =
        new StaleRecoveringContainerScrubbingService(
            recoveringContainerScrubbingSvcInterval.toMillis(),
            TimeUnit.MILLISECONDS,
            recoveringContainerScrubbingServiceWorkerSize,
            recoveringContainerScrubbingServiceTimeout,
            containerSet);

    if (certClient != null && secConf.isGrpcTlsEnabled()) {
      tlsClientConfig = new GrpcTlsConfig(
          certClient.getKeyManager(),
          certClient.getTrustManager(), true);
    } else {
      tlsClientConfig = null;
    }

    initializingStatus =
        new AtomicReference<>(InitializingStatus.UNINITIALIZED);

    this.hddsDatanodeService = hddsDatanodeService;

  }

  /**
   * Shorthand constructor used for testing in non-secure context.
   */
  @VisibleForTesting
  public OzoneContainer(
      DatanodeDetails datanodeDetails, ConfigurationSource conf,
      StateContext context) throws IOException {
    this(null, datanodeDetails, conf, context, null, null);
  }

  @VisibleForTesting
  public OzoneContainer(HddsDatanodeService hddsDatanodeService, DatanodeDetails datanodeDetails,
                        ConfigurationSource conf,
                        StateContext context) throws IOException {
    this(hddsDatanodeService, datanodeDetails, conf, context, null, null);
  }

  public GrpcTlsConfig getTlsClientConfig() {
    return tlsClientConfig;
  }

  /**
   * Build's container map after volume format.
   */
  @VisibleForTesting
  public void buildContainerSet() throws IOException {
    Iterator<StorageVolume> volumeSetIterator = volumeSet.getVolumesList()
        .iterator();
    ArrayList<Thread> volumeThreads = new ArrayList<>();
    long startTime = System.currentTimeMillis();

    // Load container inspectors that may be triggered at startup based on
    // system properties set. These can inspect and possibly repair
    // containers as we iterate them here.
    ContainerInspectorUtil.load();
    String threadNamePrefix = datanodeDetails.threadNamePrefix();
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(threadNamePrefix + "ContainerReader-%d")
        .build();
    while (volumeSetIterator.hasNext()) {
      StorageVolume volume = volumeSetIterator.next();
      ContainerReader containerReader = new ContainerReader(volumeSet,
          (HddsVolume) volume, containerSet, config, true);
      Thread thread = threadFactory.newThread(containerReader);
      thread.start();
      volumeThreads.add(thread);
    }

    try {
      for (int i = 0; i < volumeThreads.size(); i++) {
        volumeThreads.get(i).join();
      }
      try (TableIterator<Long, ? extends Table.KeyValue<Long, String>> itr =
               containerSet.getContainerIdsTable().iterator()) {
        Map<Long, Long> containerIds = new HashMap<>();
        while (itr.hasNext()) {
          containerIds.put(itr.next().getKey(), 0L);
        }
        containerSet.buildMissingContainerSetAndValidate(containerIds);
      }
    } catch (InterruptedException ex) {
      LOG.error("Volume Threads Interrupted exception", ex);
      Thread.currentThread().interrupt();
    }

    // After all containers have been processed, turn off container
    // inspectors so they are not hit during normal datanode execution.
    ContainerInspectorUtil.unload();

    LOG.info("Build ContainerSet costs {}s",
        (System.currentTimeMillis() - startTime) / 1000);
  }

  /**
   * Start background daemon thread for performing container integrity checks.
   */
  private void startContainerScrub() {
    ContainerScannerConfiguration c = config.getObject(
        ContainerScannerConfiguration.class);
    if (!c.isEnabled()) {
      LOG.info("Scheduled background container scanners and " +
          "the on-demand container scanner have been disabled.");
      return;
    }

    initOnDemandContainerScanner(c);

    backgroundScanners = new LinkedList<>();
    // This config is for testing the scanners in isolation.
    if (c.isMetadataScanEnabled()) {
      initMetadataScanner(c);
    }

    // This config is for testing the scanners in isolation.
    if (c.isDataScanEnabled()) {
      initContainerScanner(c);
    }
  }

  private void initContainerScanner(ContainerScannerConfiguration c) {
    if (c.getBandwidthPerVolume() == 0L) {
      LOG.warn(VOLUME_BYTES_PER_SECOND_KEY + " is set to 0, " +
          "so background container data scanner will not start.");
      return;
    }
    dataScanners = new ArrayList<>();
    for (StorageVolume v : volumeSet.getVolumesList()) {
      BackgroundContainerDataScanner s =
          new BackgroundContainerDataScanner(c, controller, (HddsVolume) v);
      s.start();
      dataScanners.add(s);
      backgroundScanners.add(s);
    }
  }

  /**
   * We need to inject the containerController into the hddsVolume.
   * because we need to obtain the container count
   * for each disk based on the container controller.
   */
  private void initHddsVolumeContainer() {
    for (StorageVolume v : volumeSet.getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) v;
      hddsVolume.setController(controller);
    }
  }

  private void initMetadataScanner(ContainerScannerConfiguration c) {
    if (this.metadataScanner == null) {
      this.metadataScanner =
          new BackgroundContainerMetadataScanner(c, controller);
      backgroundScanners.add(metadataScanner);
    }
    this.metadataScanner.start();
  }

  private void initOnDemandContainerScanner(ContainerScannerConfiguration c) {
    if (c.getOnDemandBandwidthPerVolume() == 0L) {
      LOG.warn(ON_DEMAND_VOLUME_BYTES_PER_SECOND_KEY + " is set to 0, " +
          "so the on-demand container data scanner will not start.");
      return;
    }
    OnDemandContainerDataScanner.init(c, controller);
  }

  /**
   * Stop the scanner thread and wait for thread to die.
   */
  private void stopContainerScrub() {
    if (metadataScanner == null) {
      return;
    }
    metadataScanner.shutdown();
    metadataScanner = null;

    if (dataScanners == null) {
      return;
    }
    for (BackgroundContainerDataScanner s : dataScanners) {
      s.shutdown();
    }
    OnDemandContainerDataScanner.shutdown();
  }

  @VisibleForTesting
  public void pauseContainerScrub() {
    backgroundScanners.forEach(AbstractBackgroundContainerScanner::pause);
  }

  @VisibleForTesting
  public void resumeContainerScrub() {
    backgroundScanners.forEach(AbstractBackgroundContainerScanner::unpause);
  }

  /**
   * Starts serving requests to ozone container.
   *
   * @throws IOException
   */
  public void start(String clusterId) throws IOException {
    // If SCM HA is enabled, OzoneContainer#start() will be called multi-times
    // from VersionEndpointTask. The first call should do the initializing job,
    // the successive calls should wait until OzoneContainer is initialized.
    if (!initializingStatus.compareAndSet(
        InitializingStatus.UNINITIALIZED, InitializingStatus.INITIALIZING)) {

      // wait OzoneContainer to finish its initializing.
      while (initializingStatus.get() != InitializingStatus.INITIALIZED) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("Ignore. OzoneContainer already started.");
      return;
    }

    buildContainerSet();

    // Start background volume checks, which will begin after the configured
    // delay.
    volumeChecker.start();
    // Do an immediate check of all volumes to ensure datanode health before
    // proceeding.
    volumeSet.checkAllVolumes();
    metaVolumeSet.checkAllVolumes();
    // DB volume set may be null if dedicated DB volumes are not used.
    if (dbVolumeSet != null) {
      dbVolumeSet.checkAllVolumes();
    }

    LOG.info("Attempting to start container services.");
    startContainerScrub();

    replicationServer.start();
    datanodeDetails.setPort(Name.REPLICATION, replicationServer.getPort());

    hddsDispatcher.init();
    hddsDispatcher.setClusterId(clusterId);
    writeChannel.start();
    readChannel.start();
    blockDeletingService.start();
    recoveringContainerScrubbingService.start();

    initHddsVolumeContainer();

    // mark OzoneContainer as INITIALIZED.
    initializingStatus.set(InitializingStatus.INITIALIZED);
  }

  /**
   * Stop Container Service on the datanode.
   */
  public void stop() {
    //TODO: at end of container IO integration work.
    LOG.info("Attempting to stop container services.");
    stopContainerScrub();
    replicationServer.stop();
    writeChannel.stop();
    readChannel.stop();
    this.handlers.values().forEach(Handler::stop);
    hddsDispatcher.shutdown();
    volumeChecker.shutdownAndWait(0, TimeUnit.SECONDS);
    volumeSet.shutdown();
    metaVolumeSet.shutdown();
    if (dbVolumeSet != null) {
      dbVolumeSet.shutdown();
    }
    if (dbCompactionExecutorService != null) {
      dbCompactionExecutorService.shutdown();
    }
    blockDeletingService.shutdown();
    recoveringContainerScrubbingService.shutdown();
    IOUtils.closeQuietly(metrics);
    ContainerMetrics.remove();
    if (this.witnessedContainerMetadataStore != null) {
      try {
        this.witnessedContainerMetadataStore.stop();
      } catch (Exception e) {
        LOG.error("Error while stopping witnessedContainerMetadataStore. Status of store: {}",
            witnessedContainerMetadataStore.isClosed(), e);
      }
      this.witnessedContainerMetadataStore = null;
    }
  }

  public void handleVolumeFailures() throws StorageContainerException {
    if (containerSet != null) {
      containerSet.handleVolumeFailures(context);
    }
  }

  @VisibleForTesting
  public ContainerSet getContainerSet() {
    return containerSet;
  }

  /**
   * Returns container report.
   *
   * @return - container report.
   */

  public PipelineReportsProto getPipelineReport() {
    PipelineReportsProto.Builder pipelineReportsProto =
        PipelineReportsProto.newBuilder();
    pipelineReportsProto.addAllPipelineReport(writeChannel.getPipelineReport());
    return pipelineReportsProto.build();
  }

  public XceiverServerSpi getWriteChannel() {
    return writeChannel;
  }

  public XceiverServerSpi getReadChannel() {
    return readChannel;
  }

  public ContainerController getController() {
    return controller;
  }

  /**
   * Returns node report of container storage usage.
   */
  public StorageContainerDatanodeProtocolProtos.NodeReportProto getNodeReport()
          throws IOException {
    StorageLocationReport[] reports = volumeSet.getStorageReport();
    StorageContainerDatanodeProtocolProtos.NodeReportProto.Builder nrb
            = StorageContainerDatanodeProtocolProtos.
            NodeReportProto.newBuilder();
    for (int i = 0; i < reports.length; i++) {
      nrb.addStorageReport(reports[i].getProtoBufMessage(config));
    }

    StorageLocationReport[] metaReports = metaVolumeSet.getStorageReport();
    for (int i = 0; i < metaReports.length; i++) {
      nrb.addMetadataStorageReport(
          metaReports[i].getMetadataProtoBufMessage());
    }

    if (dbVolumeSet != null) {
      StorageLocationReport[] dbReports = dbVolumeSet.getStorageReport();
      for (int i = 0; i < dbReports.length; i++) {
        nrb.addDbStorageReport(dbReports[i].getProtoBufMessage());
      }
    }

    collectReplicationSupervisorMetrics(nrb);

    return nrb.build();
  }

  @VisibleForTesting
  public ContainerDispatcher getDispatcher() {
    return this.hddsDispatcher;
  }

  public MutableVolumeSet getVolumeSet() {
    return volumeSet;
  }

  public MutableVolumeSet getMetaVolumeSet() {
    return metaVolumeSet;
  }

  public MutableVolumeSet getDbVolumeSet() {
    return dbVolumeSet;
  }

  public ContainerMetrics getMetrics() {
    return metrics;
  }

  public BlockDeletingService getBlockDeletingService() {
    return blockDeletingService;
  }

  public ReplicationServer getReplicationServer() {
    return replicationServer;
  }

  public void compactDb() {
    for (StorageVolume volume : volumeSet.getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) volume;
      CompletableFuture.runAsync(hddsVolume::compactDb,
          dbCompactionExecutorService);
    }
  }

  @VisibleForTesting
  public void setHddsDatanodeService(HddsDatanodeService hddsDatanodeService) {
    this.hddsDatanodeService = hddsDatanodeService;
  }

  private void collectReplicationSupervisorMetrics(StorageContainerDatanodeProtocolProtos.NodeReportProto.Builder nrb) {
    List<HddsProtos.KeyValue> containerReplicationMetrics = new ArrayList<>();

    ReplicationSupervisor replicationSupervisor = hddsDatanodeService.getDatanodeStateMachine().getSupervisor();
    MeasuredReplicator pushReplicatorMetrics = hddsDatanodeService.getDatanodeStateMachine()
        .getPushReplicatorWithMetrics();

    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("numInFlightReplications")
            .setValue(String.valueOf(replicationSupervisor.getInFlightReplications(ReplicationTask.class))).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("numQueuedReplications")
            .setValue(String.valueOf(replicationSupervisor.getQueueSize())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("numRequestedReplications")
            .setValue(String.valueOf(replicationSupervisor.getReplicationRequestCount())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("numSuccessReplications")
            .setValue(String.valueOf(replicationSupervisor.getReplicationSuccessCount())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("numFailedReplications")
            .setValue(String.valueOf(replicationSupervisor.getReplicationFailureCount())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("numTimeoutReplications")
            .setValue(String.valueOf(replicationSupervisor.getReplicationTimeoutCount())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("numSkippedReplications")
            .setValue(String.valueOf(replicationSupervisor.getReplicationSkippedCount())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("maxReplicationStreams")
            .setValue(String.valueOf(replicationSupervisor.getMaxReplicationStreams())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("transferredBytes")
            .setValue(String.valueOf(pushReplicatorMetrics.getTransferredBytes().value())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("successTime")
            .setValue(String.valueOf(pushReplicatorMetrics.getSuccessTime().value())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("failureBytes")
            .setValue(String.valueOf(pushReplicatorMetrics.getFailureBytes().value())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("failureTime")
            .setValue(String.valueOf(pushReplicatorMetrics.getFailureTime().value())).build());
    containerReplicationMetrics.add(
        HddsProtos.KeyValue.newBuilder()
            .setKey("queueTime")
            .setValue(String.valueOf(pushReplicatorMetrics.getQueueTime().value())).build());
    nrb.addAllReplicationMetrics(containerReplicationMetrics);
  }

}
