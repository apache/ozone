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

package org.apache.hadoop.ozone.recon.spi.impl;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort.Type;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.metrics.OzoneManagerSyncMetrics;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdatesHandler;
import org.apache.hadoop.ozone.recon.tasks.OMUpdateEventBatch;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LIMIT_DEFUALT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LOOP_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LOOP_LIMIT_DEFUALT;
import static org.apache.ratis.proto.RaftProtos.RaftPeerRole.LEADER;

import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
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
  private String omDBSnapshotUrl;

  private OzoneManagerProtocol ozoneManagerClient;
  private final OzoneConfiguration configuration;
  private ScheduledExecutorService scheduler;

  private ReconOMMetadataManager omMetadataManager;
  private ReconTaskController reconTaskController;
  private ReconTaskStatusDao reconTaskStatusDao;
  private ReconUtils reconUtils;
  private OzoneManagerSyncMetrics metrics;

  private long deltaUpdateLimit;
  private int deltaUpdateLoopLimit;

  private AtomicBoolean isSyncDataFromOMRunning;

  /**
   * OM Snapshot related task names.
   */
  public enum OmSnapshotTaskName {
    OmSnapshotRequest,
    OmDeltaRequest
  }

  @Inject
  public OzoneManagerServiceProviderImpl(
      OzoneConfiguration configuration,
      ReconOMMetadataManager omMetadataManager,
      ReconTaskController reconTaskController,
      ReconUtils reconUtils,
      OzoneManagerProtocol ozoneManagerClient) {

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
        RECON_OM_DELTA_UPDATE_LIMIT_DEFUALT);
    int deltaUpdateLoopLimits = configuration.getInt(
        RECON_OM_DELTA_UPDATE_LOOP_LIMIT,
        RECON_OM_DELTA_UPDATE_LOOP_LIMIT_DEFUALT);

    omSnapshotDBParentDir = reconUtils.getReconDbDir(configuration,
        OZONE_RECON_OM_SNAPSHOT_DB_DIR);

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
    this.reconTaskStatusDao = reconTaskController.getReconTaskStatusDao();
    this.ozoneManagerClient = ozoneManagerClient;
    this.configuration = configuration;
    this.metrics = OzoneManagerSyncMetrics.create();
    this.deltaUpdateLimit = deltaUpdateLimits;
    this.deltaUpdateLoopLimit = deltaUpdateLoopLimits;
    this.isSyncDataFromOMRunning = new AtomicBoolean();
  }

  public void registerOMDBTasks() {
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
        OmSnapshotTaskName.OmDeltaRequest.name(),
        System.currentTimeMillis(), getCurrentOMDBSequenceNumber());
    if (!reconTaskStatusDao.existsById(
        OmSnapshotTaskName.OmDeltaRequest.name())) {
      reconTaskStatusDao.insert(reconTaskStatusRecord);
      LOG.info("Registered {} task ",
          OmSnapshotTaskName.OmDeltaRequest.name());
    }

    reconTaskStatusRecord = new ReconTaskStatus(
        OmSnapshotTaskName.OmSnapshotRequest.name(),
        System.currentTimeMillis(), getCurrentOMDBSequenceNumber());
    if (!reconTaskStatusDao.existsById(
        OmSnapshotTaskName.OmSnapshotRequest.name())) {
      reconTaskStatusDao.insert(reconTaskStatusRecord);
      LOG.info("Registered {} task ",
          OmSnapshotTaskName.OmSnapshotRequest.name());
    }
  }

  @Override
  public OMMetadataManager getOMMetadataManagerInstance() {
    return omMetadataManager;
  }

  @Override
  public void start() {
    LOG.info("Starting Ozone Manager Service Provider.");
    scheduler = Executors.newScheduledThreadPool(1);
    registerOMDBTasks();
    try {
      omMetadataManager.start(configuration);
    } catch (IOException ioEx) {
      LOG.error("Error staring Recon OM Metadata Manager.", ioEx);
    }
    reconTaskController.start();
    long initialDelay = configuration.getTimeDuration(
        OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
        configuration.get(
            ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
            OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT),
        TimeUnit.MILLISECONDS);
    startSyncDataFromOM(initialDelay);
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
        boolean isSuccess = syncDataFromOM();
        if (!isSuccess) {
          LOG.debug("OM DB sync is already running.");
        }
      } catch (Throwable t) {
        LOG.error("Unexpected exception while syncing data from OM.", t);
      }
    },
        initialDelay,
        interval,
        TimeUnit.MILLISECONDS);
  }

  private void stopSyncDataFromOMThread() {
    scheduler.shutdownNow();
    LOG.debug("Shutdown the OM DB sync scheduler.");
  }

  @Override
  public boolean triggerSyncDataFromOMImmediately() {
    if (!isSyncDataFromOMRunning.get()) {
      // We are shutting down the scheduler and then starting another one,
      // setting the initialDelay to 0, which triggers an OM DB sync
      // immediately.
      stopSyncDataFromOMThread();
      scheduler = Executors.newScheduledThreadPool(1);
      startSyncDataFromOM(0L);
      return true;
    } else {
      LOG.debug("OM DB sync is already running.");
    }
    return false;
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Ozone Manager Service Provider.");
    reconTaskController.stop();
    omMetadataManager.stop();
    scheduler.shutdownNow();
    metrics.unRegister();
    connectionFactory.destroy();
  }

  /**
   * Find the OM leader's address to get the snapshot from.
   */
  @VisibleForTesting
  public String getOzoneManagerSnapshotUrl() throws IOException {
    if (!configuration.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, false)) {
      return omDBSnapshotUrl;
    }
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
  DBCheckpoint getOzoneManagerDBSnapshot() {
    String snapshotFileName = RECON_OM_SNAPSHOT_DB + "_" +
        System.currentTimeMillis();
    File targetFile = new File(omSnapshotDBParentDir, snapshotFileName +
        ".tar.gz");
    try {
      SecurityUtil.doAsLoginUser(() -> {
        try (InputStream inputStream = reconUtils.makeHttpCall(
            connectionFactory, getOzoneManagerSnapshotUrl(),
            isOmSpnegoEnabled()).getInputStream()) {
          FileUtils.copyInputStreamToFile(inputStream, targetFile);
        }
        return null;
      });
      // Untar the checkpoint file.
      Path untarredDbDir = Paths.get(omSnapshotDBParentDir.getAbsolutePath(),
          snapshotFileName);
      reconUtils.untarCheckpointFile(targetFile, untarredDbDir);
      FileUtils.deleteQuietly(targetFile);

      // Currently, OM DB type is not configurable. Hence, defaulting to
      // RocksDB.
      return new RocksDBCheckpoint(untarredDbDir);
    } catch (IOException e) {
      LOG.error("Unable to obtain Ozone Manager DB Snapshot. ", e);
    }
    return null;
  }

  /**
   * Update Local OM DB with new OM DB snapshot.
   * @throws IOException
   */
  @VisibleForTesting
  boolean updateReconOmDBWithNewSnapshot() throws IOException {
    // Obtain the current DB snapshot from OM and
    // update the in house OM metadata managed DB instance.
    long startTime = Time.monotonicNow();
    DBCheckpoint dbSnapshot = getOzoneManagerDBSnapshot();
    metrics.updateSnapshotRequestLatency(Time.monotonicNow() - startTime);
    if (dbSnapshot != null && dbSnapshot.getCheckpointLocation() != null) {
      LOG.info("Got new checkpoint from OM : " +
          dbSnapshot.getCheckpointLocation());
      try {
        omMetadataManager.updateOmDB(
            dbSnapshot.getCheckpointLocation().toFile());
        return true;
      } catch (IOException e) {
        LOG.error("Unable to refresh Recon OM DB Snapshot. ", e);
      }
    } else {
      LOG.error("Null snapshot location got from OM.");
    }
    return false;
  }

  /**
   * Get Delta updates from OM through RPC call and apply to local OM DB as
   * well as accumulate in a buffer.
   * @param fromSequenceNumber from sequence number to request from.
   * @param omdbUpdatesHandler OM DB updates handler to buffer updates.
   * @throws IOException when OM RPC request fails.
   * @throws RocksDBException when writing to RocksDB fails.
   */
  @VisibleForTesting
  void getAndApplyDeltaUpdatesFromOM(
      long fromSequenceNumber, OMDBUpdatesHandler omdbUpdatesHandler)
      throws IOException, RocksDBException {
    int loopCount = 0;
    LOG.info("OriginalFromSequenceNumber : {} ", fromSequenceNumber);
    long deltaUpdateCnt = Long.MAX_VALUE;
    long inLoopStartSequenceNumber = fromSequenceNumber;
    long inLoopLatestSequenceNumber;
    while (loopCount < deltaUpdateLoopLimit &&
        deltaUpdateCnt >= deltaUpdateLimit) {
      innerGetAndApplyDeltaUpdatesFromOM(
          inLoopStartSequenceNumber, omdbUpdatesHandler);
      inLoopLatestSequenceNumber = getCurrentOMDBSequenceNumber();
      deltaUpdateCnt = inLoopLatestSequenceNumber - inLoopStartSequenceNumber;
      inLoopStartSequenceNumber = inLoopLatestSequenceNumber;
      loopCount++;
    }
    LOG.info("Delta updates received from OM : {} loops, {} records", loopCount,
        getCurrentOMDBSequenceNumber() - fromSequenceNumber
    );
  }

  /**
   * Get Delta updates from OM through RPC call and apply to local OM DB as
   * well as accumulate in a buffer.
   * @param fromSequenceNumber from sequence number to request from.
   * @param omdbUpdatesHandler OM DB updates handler to buffer updates.
   * @throws IOException when OM RPC request fails.
   * @throws RocksDBException when writing to RocksDB fails.
   */
  @VisibleForTesting
  void innerGetAndApplyDeltaUpdatesFromOM(long fromSequenceNumber,
      OMDBUpdatesHandler omdbUpdatesHandler)
      throws IOException, RocksDBException {
    DBUpdatesRequest dbUpdatesRequest = DBUpdatesRequest.newBuilder()
        .setSequenceNumber(fromSequenceNumber)
        .setLimitCount(deltaUpdateLimit)
        .build();
    DBUpdates dbUpdates = ozoneManagerClient.getDBUpdates(dbUpdatesRequest);
    int numUpdates = 0;
    long latestSequenceNumberOfOM = -1L;
    if (null != dbUpdates && dbUpdates.getCurrentSequenceNumber() != -1) {
      latestSequenceNumberOfOM = dbUpdates.getLatestSequenceNumber();
      RDBStore rocksDBStore = (RDBStore) omMetadataManager.getStore();
      final RocksDatabase rocksDB = rocksDBStore.getDb();
      numUpdates = dbUpdates.getData().size();
      if (numUpdates > 0) {
        metrics.incrNumUpdatesInDeltaTotal(numUpdates);
      }
      for (byte[] data : dbUpdates.getData()) {
        try (ManagedWriteBatch writeBatch = new ManagedWriteBatch(data)) {
          writeBatch.iterate(omdbUpdatesHandler);
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
        latestSequenceNumberOfOM - getCurrentOMDBSequenceNumber();
    metrics.setSequenceNumberLag(lag);
    LOG.info("Number of updates received from OM : {}, " +
            "SequenceNumber diff: {}, SequenceNumber Lag from OM {}.",
        numUpdates, getCurrentOMDBSequenceNumber() - fromSequenceNumber, lag);
  }

  /**
   * Based on current state of Recon's OM DB, we either get delta updates or
   * full snapshot from Ozone Manager.
   */
  @VisibleForTesting
  public boolean syncDataFromOM() {
    if (isSyncDataFromOMRunning.compareAndSet(false, true)) {
      try {
        LOG.info("Syncing data from Ozone Manager.");
        long currentSequenceNumber = getCurrentOMDBSequenceNumber();
        LOG.debug("Seq number of Recon's OM DB : {}", currentSequenceNumber);
        boolean fullSnapshot = false;

        if (currentSequenceNumber <= 0) {
          fullSnapshot = true;
        } else {
          try (OMDBUpdatesHandler omdbUpdatesHandler =
              new OMDBUpdatesHandler(omMetadataManager)) {
            LOG.info("Obtaining delta updates from Ozone Manager");
            // Get updates from OM and apply to local Recon OM DB.
            getAndApplyDeltaUpdatesFromOM(currentSequenceNumber,
                omdbUpdatesHandler);
            // Update timestamp of successful delta updates query.
            ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
                OmSnapshotTaskName.OmDeltaRequest.name(),
                System.currentTimeMillis(), getCurrentOMDBSequenceNumber());
            reconTaskStatusDao.update(reconTaskStatusRecord);

            // Pass on DB update events to tasks that are listening.
            reconTaskController.consumeOMEvents(new OMUpdateEventBatch(
                omdbUpdatesHandler.getEvents()), omMetadataManager);
          } catch (InterruptedException intEx) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            metrics.incrNumDeltaRequestsFailed();
            LOG.warn("Unable to get and apply delta updates from OM.", e);
            fullSnapshot = true;
          }
        }

        if (fullSnapshot) {
          try {
            metrics.incrNumSnapshotRequests();
            LOG.info("Obtaining full snapshot from Ozone Manager");
            // Update local Recon OM DB to new snapshot.
            boolean success = updateReconOmDBWithNewSnapshot();
            // Update timestamp of successful delta updates query.
            if (success) {
              ReconTaskStatus reconTaskStatusRecord =
                  new ReconTaskStatus(
                      OmSnapshotTaskName.OmSnapshotRequest.name(),
                      System.currentTimeMillis(),
                      getCurrentOMDBSequenceNumber());
              reconTaskStatusDao.update(reconTaskStatusRecord);

              // Reinitialize tasks that are listening.
              LOG.info("Calling reprocess on Recon tasks.");
              reconTaskController.reInitializeTasks(omMetadataManager);
            }
          } catch (InterruptedException intEx) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            metrics.incrNumSnapshotRequestsFailed();
            LOG.error("Unable to update Recon's metadata with new OM DB. ", e);
          }
        }
      } finally {
        isSyncDataFromOMRunning.set(false);
      }
    } else {
      LOG.debug("OM DB sync is already running.");
      return false;
    }
    return true;
  }

  /**
   * Get OM RocksDB's latest sequence number.
   * @return latest sequence number.
   */
  private long getCurrentOMDBSequenceNumber() {
    return omMetadataManager.getLastSequenceNumberFromDB();
  }

  public OzoneManagerSyncMetrics getMetrics() {
    return metrics;
  }
}

