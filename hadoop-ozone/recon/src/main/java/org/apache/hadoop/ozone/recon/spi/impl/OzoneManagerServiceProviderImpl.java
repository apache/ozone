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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DB_DIRS_PERMISSIONS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.STAGING;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LAG_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LAG_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconUtils.convertNumericToSymbolic;
import static org.apache.ratis.proto.RaftProtos.RaftPeerRole.LEADER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort.Type;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.TarExtractor;
import org.apache.hadoop.ozone.recon.metrics.OzoneManagerSyncMetrics;
import org.apache.hadoop.ozone.recon.metrics.ReconSyncMetrics;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdatesHandler;
import org.apache.hadoop.ozone.recon.tasks.OMUpdateEventBatch;
import org.apache.hadoop.ozone.recon.tasks.ReconOmTask;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskReInitializationEvent;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Time;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the OzoneManager Service provider.
 */
@Singleton
public class OzoneManagerServiceProviderImpl
    implements OzoneManagerServiceProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerServiceProviderImpl.class);
  private URLConnectionFactory connectionFactory;

  private File omSnapshotDBParentDir = null;
  private File reconDbDir = null;
  private String omDBSnapshotUrl;

  private OzoneManagerProtocol ozoneManagerClient;
  private final OzoneConfiguration configuration;
  private ScheduledExecutorService scheduler;

  private ReconOMMetadataManager omMetadataManager;
  private ReconTaskController reconTaskController;
  private ReconUtils reconUtils;
  private OzoneManagerSyncMetrics metrics;
  private ReconSyncMetrics reconSyncMetrics;

  private final long deltaUpdateLimit;
  private final long omDBLagThreshold;

  private AtomicBoolean isSyncDataFromOMRunning;
  private final String threadNamePrefix;
  private ThreadFactory threadFactory;
  private ReconContext reconContext;
  private ReconTaskStatusUpdaterManager taskStatusUpdaterManager;
  private TarExtractor tarExtractor;

  /**
   * OM Snapshot related task names.
   */
  public enum OmSnapshotTaskName {
    OmSnapshotRequest,
    OmDeltaRequest
  }

  @Inject
  @SuppressWarnings("checkstyle:ParameterNumber")
  public OzoneManagerServiceProviderImpl(
      OzoneConfiguration configuration,
      ReconOMMetadataManager omMetadataManager,
      ReconTaskController reconTaskController,
      ReconUtils reconUtils,
      OzoneManagerProtocol ozoneManagerClient,
      ReconContext reconContext,
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {

    int connectionTimeout = (int) configuration.getTimeDuration(
        OZONE_RECON_OM_CONNECTION_TIMEOUT,
        configuration.get(
            ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT,
            OZONE_RECON_OM_CONNECTION_TIMEOUT_DEFAULT),
        TimeUnit.MILLISECONDS);
    int connectionRequestTimeout = (int)configuration.getTimeDuration(
        OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT,
        configuration.get(
            ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT,
            OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT),
        TimeUnit.MILLISECONDS
    );

    connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(connectionTimeout,
            connectionRequestTimeout, configuration);

    String ozoneManagerHttpAddress = configuration.get(OMConfigKeys
        .OZONE_OM_HTTP_ADDRESS_KEY);

    String ozoneManagerHttpsAddress = configuration.get(OMConfigKeys
        .OZONE_OM_HTTPS_ADDRESS_KEY);

    long deltaUpdateLimits = configuration.getLong(RECON_OM_DELTA_UPDATE_LIMIT,
        RECON_OM_DELTA_UPDATE_LIMIT_DEFAULT);

    omSnapshotDBParentDir = reconUtils.getReconDbDir(configuration,
        OZONE_RECON_OM_SNAPSHOT_DB_DIR);
    reconDbDir = reconUtils.getReconDbDir(configuration,
        ReconConfigKeys.OZONE_RECON_DB_DIR);

    HttpConfig.Policy policy = HttpConfig.getHttpPolicy(configuration);

    omDBSnapshotUrl = "http://" + ozoneManagerHttpAddress +
        OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;

    if (policy.isHttpsEnabled()) {
      omDBSnapshotUrl = "https://" + ozoneManagerHttpsAddress +
          OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
    }

    boolean flushParam = configuration.getBoolean(
        OZONE_RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM,
        configuration.getBoolean(
            ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM,
            false)
    );

    if (flushParam) {
      omDBSnapshotUrl += "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
    }

    this.reconUtils = reconUtils;
    this.omMetadataManager = omMetadataManager;
    this.reconTaskController = reconTaskController;
    this.ozoneManagerClient = ozoneManagerClient;
    this.configuration = configuration;
    this.metrics = OzoneManagerSyncMetrics.create();
    this.reconSyncMetrics = ReconSyncMetrics.create();
    this.deltaUpdateLimit = deltaUpdateLimits;
    this.isSyncDataFromOMRunning = new AtomicBoolean();
    this.threadNamePrefix =
        reconUtils.getReconNodeDetails(configuration).threadNamePrefix();
    this.threadFactory =
        new ThreadFactoryBuilder().setNameFormat(threadNamePrefix + "SyncOM-%d")
            .build();
    // Number of parallel workers
    int omDBTarProcessorThreadCount = Math.max(64, Runtime.getRuntime().availableProcessors());
    this.reconContext = reconContext;
    this.taskStatusUpdaterManager = taskStatusUpdaterManager;
    this.omDBLagThreshold = configuration.getLong(RECON_OM_DELTA_UPDATE_LAG_THRESHOLD,
        RECON_OM_DELTA_UPDATE_LAG_THRESHOLD_DEFAULT);
    this.tarExtractor = new TarExtractor(omDBTarProcessorThreadCount, threadNamePrefix);
  }

  @Override
  public OMMetadataManager getOMMetadataManagerInstance() {
    return omMetadataManager;
  }

  @Override
  public void start() {
    LOG.info("Starting Ozone Manager Service Provider.");
    scheduler = Executors.newScheduledThreadPool(1, threadFactory);
    try {
      tarExtractor.start();
      omMetadataManager.start(configuration);
    } catch (IOException ioEx) {
      LOG.error("Error starting Recon OM Metadata Manager.", ioEx);
      reconContext.updateHealthStatus(new AtomicBoolean(false));
      reconContext.updateErrors(ReconContext.ErrorCode.INTERNAL_ERROR);
    } catch (RuntimeException runtimeException) {
      LOG.warn("Unexpected runtime error starting Recon OM Metadata Manager.", runtimeException);
      LOG.warn("Trying to delete existing recon OM snapshot DB and fetch new one.");
      metrics.incrNumSnapshotRequests();
      LOG.info("Fetching full snapshot from Ozone Manager");
      // Update local Recon OM DB to new snapshot.
      try {
        boolean success = updateReconOmDBWithNewSnapshot();
        if (success) {
          LOG.info("Successfully fetched a full snapshot from Ozone Manager");
        } else {
          LOG.error("Failed fetching a full snapshot from Ozone Manager");
        }
      } catch (IOException e) {
        LOG.error("Unexpected IOException occurred while trying to fetch a full snapshot", e);
        throw new RuntimeException(runtimeException);
      }
    }
    reconTaskController.updateOMMetadataManager(omMetadataManager);
    reconTaskController.start();
    long initialDelay = configuration.getTimeDuration(
        OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
        configuration.get(
            ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
            OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT),
        TimeUnit.MILLISECONDS);
    // Initialize recon om tasks for any first time initialization of resources.
    reconTaskController.getRegisteredTasks()
        .values()
        .forEach(ReconOmTask::init);

    // Verify if 'OmDeltaRequest' task's lastUpdatedSeqNumber number not matching with
    // lastUpdatedSeqNumber number for any of the OM task, then just run reprocess for such tasks.
    ReconTaskStatusUpdater deltaTaskStatusUpdater =
        taskStatusUpdaterManager.getTaskStatusUpdater(OmSnapshotTaskName.OmDeltaRequest.name());
    ReconTaskStatusUpdater fullSnapshotReconTaskUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(
        OmSnapshotTaskName.OmSnapshotRequest.name());

    Map<String, ReconOmTask> reconOmTaskMap = reconTaskController.getRegisteredTasks()
        .entrySet()
        .stream()
        .filter(entry -> {
          String taskName = entry.getKey();
          ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(taskName);

          return !taskName.equals(OmSnapshotTaskName.OmDeltaRequest.name())  // Condition 1
              && !taskName.equals(OmSnapshotTaskName.OmSnapshotRequest.name())  // Condition 2
              &&
              taskStatusUpdater.getLastUpdatedSeqNumber().compareTo(
                  deltaTaskStatusUpdater.getLastUpdatedSeqNumber()) < 0; // Condition 3
        })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));  // Collect into desired Map
    if (!reconOmTaskMap.isEmpty()) {
      LOG.info("Task name and last updated sequence number of tasks, that are not matching with " +
          "the last updated sequence number of OmDeltaRequest task:\n");
      LOG.info("{} -> {}", deltaTaskStatusUpdater.getTaskName(), deltaTaskStatusUpdater.getLastUpdatedSeqNumber());
      reconOmTaskMap.keySet()
          .forEach(taskName -> {
            LOG.info("{} -> {}", taskName,
                taskStatusUpdaterManager.getTaskStatusUpdater(taskName).getLastUpdatedSeqNumber());

          });
      LOG.info("Re-initializing all tasks again (not just above failed delta tasks) based on updated OM DB snapshot " +
          "and last updated sequence number because fresh staging DB needs to be created for all tasks.");
      // Reinitialize tasks that are listening.
      LOG.info("Queueing async reinitialization events during startup.");
      ReconTaskController.ReInitializationResult result = reconTaskController.queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
      if (result != ReconTaskController.ReInitializationResult.SUCCESS) {
        LOG.error(
            "Failed to queue reinitialization event for manual trigger at startup (result: {}), " +
                "failing the snapshot operation", result);
        metrics.incrNumSnapshotRequestsFailed();
        fullSnapshotReconTaskUpdater.setLastTaskRunStatus(-1);
        fullSnapshotReconTaskUpdater.recordRunCompletion();
        reconContext.updateHealthStatus(new AtomicBoolean(false));
        reconContext.updateErrors(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
        throw new RuntimeException("Failed to queue reinitialization event for manual trigger at startup");
      }
    }
    startSyncDataFromOM(initialDelay);
    LOG.info("Ozone Manager Service Provider is started.");
  }

  private void startSyncDataFromOM(long initialDelay) {
    long interval = configuration.getTimeDuration(
        OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY,
        configuration.get(
            ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY,
            OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT),
        TimeUnit.MILLISECONDS);
    LOG.debug("Started the OM DB sync scheduler.");
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        LOG.info("Last known sequence number before sync: {}", getCurrentOMDBSequenceNumber());
        boolean isSuccess = syncDataFromOM();
        if (!isSuccess) {
          LOG.debug("OM DB sync is already running, or encountered an error while trying to sync data.");
        }
        LOG.info("Sequence number after sync: {}", getCurrentOMDBSequenceNumber());
      } catch (Throwable t) {
        LOG.error("Unexpected exception while syncing data from OM.", t);
      }
    },
        initialDelay,
        interval,
        TimeUnit.MILLISECONDS);
  }

  private void stopSyncDataFromOMThread() {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.error("OM sync scheduler failed to terminate");
        }
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
    tarExtractor.stop();
    LOG.debug("Shutdown the OM DB sync scheduler.");
  }

  @Override
  public boolean triggerSyncDataFromOMImmediately() {
    if (!isSyncDataFromOMRunning.get()) {
      // We are shutting down the scheduler and then starting another one,
      // setting the initialDelay to 0, which triggers an OM DB sync
      // immediately.
      stopSyncDataFromOMThread();
      scheduler = Executors.newScheduledThreadPool(1, threadFactory);
      tarExtractor.start();
      startSyncDataFromOM(0L);
      return true;
    } else {
      LOG.info("OM DB sync is already running when trying to trigger OM DB sync manually.");
    }
    return false;
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Ozone Manager Service Provider.");
    reconTaskController.stop();
    omMetadataManager.stop();
    scheduler.shutdownNow();
    tarExtractor.stop();
    metrics.unRegister();
    reconSyncMetrics.unRegister();
    connectionFactory.destroy();
  }

  /**
   * Find the OM leader's address to get the snapshot from.
   */
  @VisibleForTesting
  public String getOzoneManagerSnapshotUrl() throws IOException {
    String omLeaderUrl = omDBSnapshotUrl;
    List<org.apache.hadoop.ozone.om.helpers.ServiceInfo> serviceList =
        ozoneManagerClient.getServiceList();
    HttpConfig.Policy policy = HttpConfig.getHttpPolicy(configuration);
    if (!serviceList.isEmpty()) {
      for (org.apache.hadoop.ozone.om.helpers.ServiceInfo info : serviceList) {
        if (info.getNodeType().equals(HddsProtos.NodeType.OM) &&
            info.getOmRoleInfo().hasServerRole() &&
            info.getOmRoleInfo().getServerRole().equals(LEADER.name())) {
          omLeaderUrl = (policy.isHttpsEnabled() ?
              "https://" + info.getServiceAddress(Type.HTTPS) :
              "http://" + info.getServiceAddress(Type.HTTP)) +
              OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
        }
      }
    }
    return omLeaderUrl;
  }

  private boolean isOmSpnegoEnabled() {
    return configuration.get(OZONE_OM_HTTP_AUTH_TYPE, "simple")
        .equals("kerberos");
  }

  /**
   * Method to obtain current OM DB Snapshot.
   * @return DBCheckpoint instance.
   */
  @VisibleForTesting
  public DBCheckpoint getOzoneManagerDBSnapshot() {
    String snapshotFileName = RECON_OM_SNAPSHOT_DB + "_" + System.currentTimeMillis();
    Path untarredDbDir = Paths.get(omSnapshotDBParentDir.getAbsolutePath(), snapshotFileName);

    // Before fetching full snapshot again and create a new OM DB snapshot directory, check and delete
    // any existing OM DB snapshot directories under recon om db dir location and delete all such
    // om db snapshot dirs including the last known om db snapshot dir returned by reconUtils.getLastKnownDB
    File lastKnownDB = reconUtils.getLastKnownDB(omSnapshotDBParentDir, RECON_OM_SNAPSHOT_DB);
    if (lastKnownDB != null) {
      boolean existingOmSnapshotDBDeleted = FileUtils.deleteQuietly(lastKnownDB);
      if (existingOmSnapshotDBDeleted) {
        LOG.info("Successfully deleted existing OM DB snapshot directory: {}",
            lastKnownDB.getAbsolutePath());
      } else {
        LOG.warn("Failed to delete existing OM DB snapshot directory: {}",
            lastKnownDB.getAbsolutePath());
      }
    }

    // Now below cleanup operation will even remove any left over staging dirs in recon om db dir location which
    // may be left due to any previous partial extraction of tar entries and during copy sst files process by
    // tarExtractor.extractTar
    File[] leftOverStagingDirs = omSnapshotDBParentDir.listFiles(f -> f.getName().startsWith(STAGING));
    if (leftOverStagingDirs != null) {
      for (File stagingDir : leftOverStagingDirs) {
        LOG.warn("Cleaning up leftover staging folder from failed extraction: {}", stagingDir.getAbsolutePath());
        boolean stagingDirDeleted = FileUtils.deleteQuietly(stagingDir);
        if (stagingDirDeleted) {
          LOG.info("Successfully deleted leftover staging folder: {}", stagingDir.getAbsolutePath());
        } else {
          LOG.warn("Failed to delete leftover staging folder: {}", stagingDir.getAbsolutePath());
        }
      }
    }

    try {
      SecurityUtil.doAsLoginUser(() -> {
        try (InputStream inputStream = reconUtils.makeHttpCall(
            connectionFactory, getOzoneManagerSnapshotUrl(), isOmSpnegoEnabled()).getInputStream()) {
          tarExtractor.extractTar(inputStream, untarredDbDir);
        } catch (IOException | InterruptedException e) {
          reconContext.updateHealthStatus(new AtomicBoolean(false));
          reconContext.updateErrors(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
          throw new RuntimeException("Error while extracting OM DB Snapshot TAR.", e);
        }
        return null;
      });
      // Validate extracted files
      File[] sstFiles = untarredDbDir.toFile().listFiles((dir, name) -> name.endsWith(".sst"));
      if (sstFiles != null && sstFiles.length > 0) {
        LOG.info("Number of SST files found in the OM snapshot directory: {} - {}", untarredDbDir, sstFiles.length);
      }

      List<String> sstFileNames = Arrays.stream(sstFiles)
          .map(File::getName)
          .collect(Collectors.toList());
      LOG.debug("Valid SST files found: {}", sstFileNames);

      // Currently, OM DB type is not configurable. Hence, defaulting to
      // RocksDB.
      reconContext.updateHealthStatus(new AtomicBoolean(true));
      reconContext.getErrors().remove(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
      return new RocksDBCheckpoint(untarredDbDir);
    } catch (IOException e) {
      LOG.error("Unable to obtain Ozone Manager DB Snapshot.", e);
      reconContext.updateHealthStatus(new AtomicBoolean(false));
      reconContext.updateErrors(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
    }
    return null;
  }

  /**
   * Update Local OM DB with new OM DB snapshot.
   * @throws IOException
   */
  @VisibleForTesting
  boolean updateReconOmDBWithNewSnapshot() throws IOException {
    // Check permissions of the Recon DB directory
    checkAndValidateReconDbPermissions();

    // Track full DB fetch request
    reconSyncMetrics.incrFullDBFetchRequests();

    // Obtain the current DB snapshot from OM and
    // update the in house OM metadata managed DB instance.
    long startTime = Time.monotonicNow();
    DBCheckpoint dbSnapshot = getOzoneManagerDBSnapshot();
    long fullDBLatency = Time.monotonicNow() - startTime;

    reconSyncMetrics.updateFullDBRequestLatency(fullDBLatency);

    if (dbSnapshot == null) {
      LOG.error("Failed to obtain a valid DB snapshot from Ozone Manager. This could be due to " +
          "missing SST files or other fetch issues.");
      reconSyncMetrics.incrSnapshotDownloadFailures();
      return false;
    }

    if (dbSnapshot.getCheckpointLocation() == null) {
      LOG.error("Snapshot checkpoint location is null, indicating a failure to properly fetch or " +
          "store the snapshot.");
      reconSyncMetrics.incrSnapshotDownloadFailures();
      return false;
    }

    LOG.info("Attempting to update Recon OM DB with new snapshot located at: {}",
        dbSnapshot.getCheckpointLocation());
    try {
      // Calculate snapshot size
      File snapshotDir = dbSnapshot.getCheckpointLocation().toFile();
      long snapshotSize = FileUtils.sizeOfDirectory(snapshotDir);
      reconSyncMetrics.incrSnapshotSizeBytes(snapshotSize);

      omMetadataManager.updateOmDB(snapshotDir, true);

      // Track successful snapshot download
      reconSyncMetrics.incrSnapshotDownloadSuccess();

      LOG.info("Successfully updated Recon OM DB with new snapshot.");
      return true;
    } catch (IOException e) {
      LOG.error("Unable to refresh Recon OM DB Snapshot.", e);
      reconSyncMetrics.incrSnapshotDownloadFailures();
      return false;
    }
  }

  /**
   * Get Delta updates from OM through RPC call and apply to local OM DB as
   * well as accumulate in a buffer.
   *
   * @param fromSequenceNumber from sequence number to request from.
   * @param omdbUpdatesHandler OM DB updates handler to buffer updates.
   * @return lag count which tells how much Recon OM DB snapshot is lagging from OM DB.
   * @throws IOException      when OM RPC request fails.
   * @throws RocksDBException when writing to RocksDB fails.
   */
  @VisibleForTesting
  Long getAndApplyDeltaUpdatesFromOM(
      long fromSequenceNumber, OMDBUpdatesHandler omdbUpdatesHandler)
      throws IOException, RocksDBException {
    LOG.debug("OriginalFromSequenceNumber : {} ", fromSequenceNumber);
    ImmutablePair<Boolean, Long> dbUpdatesLatestSeqNumOfOMDB =
        innerGetAndApplyDeltaUpdatesFromOM(fromSequenceNumber, omdbUpdatesHandler);
    if (!dbUpdatesLatestSeqNumOfOMDB.getLeft()) {
      LOG.error(
          "Retrieve OM DB delta update failed for sequence number : {}, " +
              "so falling back to full snapshot.", fromSequenceNumber);
      throw new RocksDBException(
          "Unable to get delta updates since sequenceNumber - " +
              fromSequenceNumber);
    }
    omdbUpdatesHandler.setLatestSequenceNumber(getCurrentOMDBSequenceNumber());
    return dbUpdatesLatestSeqNumOfOMDB.getRight();
  }

  /**
   * Get Delta updates from OM through RPC call and apply to local OM DB as
   * well as accumulate in a buffer.
   *
   * @param fromSequenceNumber from sequence number to request from.
   * @param omdbUpdatesHandler OM DB updates handler to buffer updates.
   * @return Pair of dbUpdatesSuccess, lag (lag between OM and Recom)
   * @throws IOException      when OM RPC request fails.
   * @throws RocksDBException when writing to RocksDB fails.
   */
  @VisibleForTesting
  ImmutablePair<Boolean, Long> innerGetAndApplyDeltaUpdatesFromOM(long fromSequenceNumber,
                                                                  OMDBUpdatesHandler omdbUpdatesHandler)
      throws IOException, RocksDBException {
    // Track delta fetch operation
    long deltaFetchStartTime = Time.monotonicNow();

    DBUpdatesRequest dbUpdatesRequest = DBUpdatesRequest.newBuilder()
        .setSequenceNumber(fromSequenceNumber)
        .setLimitCount(deltaUpdateLimit)
        .build();
    DBUpdates dbUpdates = ozoneManagerClient.getDBUpdates(dbUpdatesRequest);

    // Update delta fetch duration
    long deltaFetchDuration = Time.monotonicNow() - deltaFetchStartTime;
    reconSyncMetrics.updateDeltaFetchDuration(deltaFetchDuration);

    int numUpdates = 0;
    long latestSequenceNumberOfOM = -1L;
    if (null != dbUpdates && dbUpdates.getCurrentSequenceNumber() != -1) {
      // Delta fetch succeeded
      reconSyncMetrics.incrDeltaFetchSuccess();

      latestSequenceNumberOfOM = dbUpdates.getLatestSequenceNumber();
      RDBStore rocksDBStore = (RDBStore) omMetadataManager.getStore();
      numUpdates = dbUpdates.getData().size();
      if (numUpdates > 0) {
        metrics.incrNumUpdatesInDeltaTotal(numUpdates);

        // Track delta data fetch size
        long totalDataSize = 0;
        for (byte[] data : dbUpdates.getData()) {
          totalDataSize += data.length;
        }
        reconSyncMetrics.incrDeltaDataFetchSize(totalDataSize);
      }

      // Track delta apply (conversion + DB apply combined)
      long deltaApplyStartTime = Time.monotonicNow();

      try {
        for (byte[] data : dbUpdates.getData()) {
          try (ManagedWriteBatch writeBatch = new ManagedWriteBatch(data)) {
            // Events gets populated in events list in OMDBUpdatesHandler with call back for put/delete/update
            writeBatch.iterate(omdbUpdatesHandler);
            // Commit the OM DB transactions in recon rocks DB and sync here.
            try (RDBBatchOperation rdbBatchOperation = rocksDBStore.initBatchOperation(writeBatch)) {
              try (ManagedWriteOptions wOpts = new ManagedWriteOptions()) {
                rocksDBStore.commitBatchOperation(rdbBatchOperation, wOpts);
              }
            }
          }
        }

        // Update delta apply duration (successful)
        long deltaApplyDuration = Time.monotonicNow() - deltaApplyStartTime;
        reconSyncMetrics.updateDeltaApplyDuration(deltaApplyDuration);

      } catch (RocksDBException | IOException e) {
        // Track delta apply failures
        reconSyncMetrics.incrDeltaApplyFailures();
        throw e;
      }
    } else {
      // Delta fetch failed
      reconSyncMetrics.incrDeltaFetchFailures();
    }
    long lag = latestSequenceNumberOfOM == -1 ? 0 :
        latestSequenceNumberOfOM - getCurrentOMDBSequenceNumber();
    metrics.setSequenceNumberLag(lag);
    LOG.info("From Sequence Number:{}, Recon DB Sequence Number: {}, Number of updates received from OM : {}, " +
            "SequenceNumber diff: {}, SequenceNumber Lag from OM {}, isDBUpdateSuccess: {}",
        fromSequenceNumber, getCurrentOMDBSequenceNumber(), numUpdates,
        getCurrentOMDBSequenceNumber() - fromSequenceNumber, lag, null != dbUpdates && dbUpdates.isDBUpdateSuccess());
    return new ImmutablePair<>(null != dbUpdates && dbUpdates.isDBUpdateSuccess(), lag);
  }

  /**
   * This method performs the syncing of data from OM.
   * <ul>
   *   <li>Initially it will fetch a snapshot of OM DB.</li>
   *   <li>If we already have data synced it will try to fetch delta updates.</li>
   *   <li>If the sync is completed successfully it will trigger other OM tasks to process events</li>
   *   <li>If there is any exception while trying to fetch delta updates, it will fall back to full snapshot update</li>
   *   <li>If there is any exception in full snapshot update it will do nothing, and return true.</li>
   *   <li>In case of an interrupt signal (irrespective of delta or snapshot sync),
   *       it will catch and mark the task as interrupted, and return false i.e. sync failed status.</li>
   * </ul>
   * @return true or false if sync operation between Recon and OM was successful or failed.
   */
  @VisibleForTesting
  @SuppressWarnings("methodlength")
  public boolean syncDataFromOM() {
    ReconTaskStatusUpdater fullSnapshotReconTaskUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(
        OmSnapshotTaskName.OmSnapshotRequest.name());
    ReconTaskStatusUpdater deltaReconTaskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(
        OmSnapshotTaskName.OmDeltaRequest.name());
    if (isSyncDataFromOMRunning.compareAndSet(false, true)) {
      try {
        long currentSequenceNumber = getCurrentOMDBSequenceNumber();
        LOG.info("Seq number of Recon's OM DB : {}", currentSequenceNumber);
        boolean fullSnapshot = false;

        if (currentSequenceNumber <= 0) {
          fullSnapshot = true;
        } else {
          // Get updates from OM and apply to local Recon OM DB and update task status in table
          deltaReconTaskStatusUpdater.recordRunStart();
          int loopCount = 0;
          long fromSequenceNumber = currentSequenceNumber;
          long diffBetweenOMDbAndReconDBSeqNumber = deltaUpdateLimit + 1;
          /**
           * This loop will continue to fetch and apply OM DB updates and with every
           * OM DB fetch request, it will fetch {@code deltaUpdateLimit} count of DB updates.
           * It continues to fetch from OM till the lag, between OM DB WAL sequence number
           * and Recon OM DB snapshot WAL sequence number, is less than this lag threshold value.
           * In high OM write TPS cluster, this simulates continuous pull from OM without any delay.
           */
          while (diffBetweenOMDbAndReconDBSeqNumber > omDBLagThreshold) {
            try (OMDBUpdatesHandler omdbUpdatesHandler =
                     new OMDBUpdatesHandler(omMetadataManager)) {

              // If interrupt was previously signalled,
              // we should check for it before starting delta update sync.
              if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted during delta update.");
              }
              diffBetweenOMDbAndReconDBSeqNumber =
                  getAndApplyDeltaUpdatesFromOM(currentSequenceNumber, omdbUpdatesHandler);
              deltaReconTaskStatusUpdater.setLastTaskRunStatus(0);
              // Keeping last updated sequence number for both full and delta tasks to be same
              // because sequence number of DB denotes and points to same OM DB copy of Recon,
              // even though two different tasks are updating the DB at different conditions, but
              // it tells the sync state with actual OM DB for the same Recon OM DB copy.
              deltaReconTaskStatusUpdater.setLastUpdatedSeqNumber(getCurrentOMDBSequenceNumber());
              fullSnapshotReconTaskUpdater.setLastUpdatedSeqNumber(getCurrentOMDBSequenceNumber());
              deltaReconTaskStatusUpdater.recordRunCompletion();
              fullSnapshotReconTaskUpdater.updateDetails();
              // Update the current OM metadata manager in task controller
              reconTaskController.updateOMMetadataManager(omMetadataManager);

              // Pass on DB update events to tasks that are listening.
              reconTaskController.consumeOMEvents(new OMUpdateEventBatch(
                  omdbUpdatesHandler.getEvents(), omdbUpdatesHandler.getLatestSequenceNumber()), omMetadataManager);

              // Check if task reinitialization is needed due to buffer overflow or task failures
              boolean bufferOverflowed = reconTaskController.hasEventBufferOverflowed();
              boolean tasksFailed = reconTaskController.hasTasksFailed();

              if (bufferOverflowed || tasksFailed) {
                ReconTaskReInitializationEvent.ReInitializationReason reason = bufferOverflowed ?
                    ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW :
                    ReconTaskReInitializationEvent.ReInitializationReason.TASK_FAILURES;

                LOG.warn("Detected condition for task reinitialization: {}, queueing async reinitialization event",
                    reason);

                markDeltaTaskStatusAsFailed(deltaReconTaskStatusUpdater);

                // Queue async reinitialization event - checkpoint creation and retry logic is handled internally
                ReconTaskController.ReInitializationResult result =
                    reconTaskController.queueReInitializationEvent(reason);

                //TODO: Create a metric to track this event buffer overflow or task failure event
                boolean triggerFullSnapshot =
                    Optional.ofNullable(result)
                        .map(r -> {
                          switch (r) {
                          case MAX_RETRIES_EXCEEDED:
                            LOG.warn(
                                "Reinitialization queue failures exceeded maximum retries, triggering full snapshot " +
                                    "fallback");
                            return true;

                          case RETRY_LATER:
                            LOG.debug("Reinitialization event queueing will be retried in next iteration");
                            return false;

                          default:
                            LOG.info("Reinitialization event successfully queued");
                            return false;
                          }
                        })
                        .orElseGet(() -> {
                          LOG.error(
                              "ReInitializationResult is null, something went wrong in queueing reinitialization " +
                                  "event");
                          return true;
                        });

                if (triggerFullSnapshot) {
                  fullSnapshot = true;
                }
              }
              currentSequenceNumber = getCurrentOMDBSequenceNumber();
              LOG.debug("Updated current sequence number: {}", currentSequenceNumber);
              loopCount++;
            } catch (InterruptedException intEx) {
              LOG.error("OM DB Delta update sync thread was interrupted and delta sync failed.");
              // We are updating the table even if it didn't run i.e. got interrupted beforehand
              // to indicate that a task was supposed to run, but it didn't.
              markDeltaTaskStatusAsFailed(deltaReconTaskStatusUpdater);
              Thread.currentThread().interrupt();
              // Since thread is interrupted, we do not fall back to snapshot sync.
              // Return with sync failed status.
              return false;
            } catch (Exception e) {
              markDeltaTaskStatusAsFailed(deltaReconTaskStatusUpdater);
              LOG.warn("Unable to get and apply delta updates from OM: {}, falling back to full snapshot",
                  e.getMessage());
              fullSnapshot = true;
            }
            if (fullSnapshot) {
              break;
            }
          }
          LOG.info("Delta updates received from OM : {} loops, {} records", loopCount,
              getCurrentOMDBSequenceNumber() - fromSequenceNumber);
        }

        if (fullSnapshot) {
          try {
            executeFullSnapshot(fullSnapshotReconTaskUpdater, deltaReconTaskStatusUpdater);
          } catch (InterruptedException intEx) {
            LOG.error("OM DB Snapshot update sync thread was interrupted.");
            fullSnapshotReconTaskUpdater.setLastTaskRunStatus(-1);
            fullSnapshotReconTaskUpdater.recordRunCompletion();
            Thread.currentThread().interrupt();
            // Mark sync status as failed.
            return false;
          } catch (Exception e) {
            metrics.incrNumSnapshotRequestsFailed();
            fullSnapshotReconTaskUpdater.setLastTaskRunStatus(-1);
            fullSnapshotReconTaskUpdater.recordRunCompletion();
            LOG.error("Unable to update Recon's metadata with new OM DB. ", e);
            // Update health status in ReconContext
            reconContext.updateHealthStatus(new AtomicBoolean(false));
            reconContext.updateErrors(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
          }
        }
        printOMDBMetaInfo();
      } finally {
        isSyncDataFromOMRunning.set(false);
      }
    } else {
      LOG.info("OM DB sync is already running in syncDataFromOM.");
      return false;
    }
    return true;
  }

  private void markDeltaTaskStatusAsFailed(ReconTaskStatusUpdater deltaReconTaskStatusUpdater) {
    metrics.incrNumDeltaRequestsFailed();
    deltaReconTaskStatusUpdater.setLastTaskRunStatus(-1);
    deltaReconTaskStatusUpdater.recordRunCompletion();
  }

  private void executeFullSnapshot(ReconTaskStatusUpdater fullSnapshotReconTaskUpdater,
                         ReconTaskStatusUpdater deltaReconTaskStatusUpdater) throws InterruptedException, IOException {
    metrics.incrNumSnapshotRequests();
    LOG.info("Obtaining full snapshot from Ozone Manager");

    // Similarly if the interrupt was signalled in between,
    // we should check before starting snapshot sync.
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("Thread interrupted during snapshot sync.");
    }

    // Update local Recon OM DB to new snapshot.
    fullSnapshotReconTaskUpdater.recordRunStart();
    boolean success = updateReconOmDBWithNewSnapshot();
    // Update timestamp of successful delta updates query.
    if (success) {
      // Keeping last updated sequence number for both full and delta tasks to be same
      // because sequence number of DB denotes and points to same OM DB copy of Recon,
      // even though two different tasks are updating the DB at different conditions, but
      // it tells the sync state with actual OM DB for the same Recon OM DB copy.
      fullSnapshotReconTaskUpdater.setLastUpdatedSeqNumber(getCurrentOMDBSequenceNumber());
      deltaReconTaskStatusUpdater.setLastUpdatedSeqNumber(getCurrentOMDBSequenceNumber());
      fullSnapshotReconTaskUpdater.setLastTaskRunStatus(0);
      fullSnapshotReconTaskUpdater.recordRunCompletion();
      deltaReconTaskStatusUpdater.updateDetails();

      // Update the current OM metadata manager in task controller
      reconTaskController.updateOMMetadataManager(omMetadataManager);

      // Reinitialize tasks that are listening.
      LOG.info("Queueing async reinitialization event instead of blocking call");
      ReconTaskController.ReInitializationResult result = reconTaskController.queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
      if (result != ReconTaskController.ReInitializationResult.SUCCESS) {
        LOG.error(
            "Failed to queue reinitialization event for manual trigger (result: {}), failing the snapshot operation",
            result);
        metrics.incrNumSnapshotRequestsFailed();
        fullSnapshotReconTaskUpdater.setLastTaskRunStatus(-1);
        fullSnapshotReconTaskUpdater.recordRunCompletion();
        reconContext.updateHealthStatus(new AtomicBoolean(false));
        reconContext.updateErrors(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
        return;
      }

      // Update health status in ReconContext
      reconContext.updateHealthStatus(new AtomicBoolean(true));
      reconContext.getErrors().remove(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
    } else {
      metrics.incrNumSnapshotRequestsFailed();
      fullSnapshotReconTaskUpdater.setLastTaskRunStatus(-1);
      fullSnapshotReconTaskUpdater.recordRunCompletion();
      // Update health status in ReconContext
      reconContext.updateHealthStatus(new AtomicBoolean(false));
      reconContext.updateErrors(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);
    }
  }

  private void printOMDBMetaInfo() {
    printTableCount("fileTable");
    printTableCount("keyTable");
  }

  private void printTableCount(String tableName) {
    Table table = omMetadataManager.getTable(tableName);
    if (table == null) {
      LOG.error("Table {} not found in OM Metadata.", tableName);
      return;
    }
    if (LOG.isDebugEnabled()) {
      try (TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator = table.iterator()) {
        long count = Iterators.size(iterator);
        LOG.debug("{} Table count: {}", tableName, count);
      } catch (IOException ioException) {
        LOG.error("Unexpected error while iterating table for table count: {}", tableName);
      }
    }
  }

  public void checkAndValidateReconDbPermissions() {
    File dbDir = new File(reconDbDir.getPath());
    if (!dbDir.exists()) {
      LOG.error("Recon DB directory does not exist: {}", dbDir.getAbsolutePath());
      return;
    }

    try {
      // Fetch expected minimum permissions from configuration
      String expectedPermissions =
          configuration.get(ReconConfigKeys.OZONE_RECON_DB_DIRS_PERMISSIONS, OZONE_RECON_DB_DIRS_PERMISSIONS_DEFAULT);
      Set<PosixFilePermission> expectedPosixPermissions =
          PosixFilePermissions.fromString(convertNumericToSymbolic(expectedPermissions));

      // Get actual permissions
      Set<PosixFilePermission> actualPermissions = Files.getPosixFilePermissions(dbDir.toPath());
      String actualPermissionsStr = PosixFilePermissions.toString(actualPermissions);

      // Check if actual permissions meet the minimum required permissions
      if (actualPermissions.containsAll(expectedPosixPermissions)) {
        LOG.info("Permissions for Recon DB directory '{}' meet the minimum required permissions '{}'",
            dbDir.getAbsolutePath(), expectedPermissions);
      } else {
        LOG.warn("Permissions for Recon DB directory '{}' are '{}', which do not meet the minimum" +
            " required permissions '{}'", dbDir.getAbsolutePath(), actualPermissionsStr, expectedPermissions);
      }
    } catch (IOException e) {
      LOG.error("Failed to retrieve permissions for Recon DB directory: {}", dbDir.getAbsolutePath(), e);
    } catch (IllegalArgumentException e) {
      LOG.error("Configuration issue: {}", e.getMessage());
    }
  }

  /**
   * Get OM RocksDB's latest sequence number.
   * @return latest sequence number.
   */
  @VisibleForTesting
  public long getCurrentOMDBSequenceNumber() {
    return omMetadataManager.getLastSequenceNumberFromDB();
  }

  public OzoneManagerSyncMetrics getMetrics() {
    return metrics;
  }

  @VisibleForTesting
  public TarExtractor getTarExtractor() {
    return tarExtractor;
  }

}

