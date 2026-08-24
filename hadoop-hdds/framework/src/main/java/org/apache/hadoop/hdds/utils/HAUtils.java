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

package org.apache.hadoop.hdds.utils;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CA_LIST_RETRY_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CA_LIST_RETRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB.ScmNodeTarget;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * utility class used by SCM and OM for HA.
 */
public final class HAUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HAUtils.class);

  private HAUtils() {
  }

  public static ScmInfo getScmInfo(OzoneConfiguration conf) throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration(conf);
    try {
      long duration = conf.getTimeDuration(OZONE_SCM_INFO_WAIT_DURATION,
          OZONE_SCM_INFO_WAIT_DURATION_DEFAULT, TimeUnit.SECONDS);
      SCMClientConfig scmClientConfig =
          configuration.getObject(SCMClientConfig.class);
      int retryCount =
          (int) (duration / (scmClientConfig.getRetryInterval() / 1000));
      // If duration is set to lesser value, fall back to actual default
      // retry count.
      if (retryCount > scmClientConfig.getRetryCount()) {
        scmClientConfig.setRetryCount(retryCount);
        configuration.setFromObject(scmClientConfig);
      }
      return getScmBlockClient(configuration).getScmInfo();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to get SCM info", e);
    }
  }

  /**
   * Add SCM to the cluster.
   * @param conf - OzoneConfiguration
   * @param request - AddSCMRequest which has details of SCM to be added.
   * @param selfId - Node Id of the SCM which is submitting the request to
   * add SCM.
   * @return true - if SCM node is added successfully, else false.
   */
  public static boolean addSCM(OzoneConfiguration conf, AddSCMRequest request,
      String selfId) throws IOException {
    OzoneConfiguration config = SCMHAUtils.removeSelfId(conf, selfId);
    try {
      return getScmBlockClient(config).addSCM(request);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to add SCM", e);
    }
  }

  /**
   * Create a scm block client.
   */
  public static ScmBlockLocationProtocol getScmBlockClient(
      OzoneConfiguration conf) {
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            new SCMBlockLocationFailoverProxyProvider(conf), conf);
    return TracingUtil
        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
            conf);
  }

  public static StorageContainerLocationProtocol getScmContainerClient(
      ConfigurationSource conf) {
    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf, null);
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                proxyProvider), StorageContainerLocationProtocol.class, conf);
    return scmContainerClient;
  }

  @VisibleForTesting
  public static StorageContainerLocationProtocol getScmContainerClient(
      ConfigurationSource conf, UserGroupInformation userGroupInformation) {
    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf,
            userGroupInformation);
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                proxyProvider), StorageContainerLocationProtocol.class, conf);
    return scmContainerClient;
  }

  public static StorageContainerLocationProtocol getScmContainerClientForNode(
      ConfigurationSource conf, ScmNodeTarget targetScmNode) {
    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf, null);
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                proxyProvider, targetScmNode), StorageContainerLocationProtocol.class, conf);
    return scmContainerClient;
  }

  /**
   * Replace the current DB with the new DB checkpoint.
   * (checkpoint in checkpointPath will not be deleted here)
   *
   * @param lastAppliedIndex the last applied index in the current SCM DB.
   * @param checkpointPath   path to the new DB checkpoint
   * @return location of backup of the original DB
   */
  public static File replaceDBWithCheckpoint(long lastAppliedIndex,
      File oldDB, Path checkpointPath, String dbPrefix) throws IOException {

    // Take a backup of the current DB
    String dbBackupName =
        dbPrefix + lastAppliedIndex + "_" + System
            .currentTimeMillis();
    File dbDir = oldDB.getParentFile();
    File dbBackup = new File(dbDir, dbBackupName);

    try {
      Files.move(oldDB.toPath(), dbBackup.toPath());
    } catch (IOException e) {
      LOG.error("Failed to create a backup of the current DB. Aborting "
          + "snapshot installation.");
      throw e;
    }

    // Move the new DB checkpoint into the metadata dir
    Path markerFile = new File(dbDir, DB_TRANSIENT_MARKER).toPath();
    try {
      // Create a Transient Marker file. This file will be deleted if the
      // checkpoint DB is successfully moved to the old DB location or if the
      // old DB backup is reset to its location. If not, then the DB is in
      // an inconsistent state and this marker file will fail it from
      // starting up.
      Files.createFile(markerFile);
      // Copy the candidate DB to real DB
      org.apache.commons.io.FileUtils.copyDirectory(checkpointPath.toFile(),
          oldDB);
      Files.deleteIfExists(markerFile);
    } catch (IOException e) {
      LOG.error("Failed to move downloaded DB checkpoint {} to metadata "
              + "directory {}. Resetting to original DB.", checkpointPath,
          oldDB.toPath());
      try {
        FileUtil.fullyDelete(oldDB);
        Files.move(dbBackup.toPath(), oldDB.toPath());
        Files.deleteIfExists(markerFile);
      } catch (IOException ex) {
        String errorMsg = "Failed to reset to original DB. SCM is in an "
            + "inconsistent state.";
        ExitUtils.terminate(1, errorMsg, ex, LOG);
      }
      throw e;
    }
    return dbBackup;
  }

  /**
   * Obtain SCMTransactionInfo from Checkpoint.
   */
  public static TransactionInfo getTrxnInfoFromCheckpoint(
      OzoneConfiguration conf, Path dbPath, DBDefinition definition)
      throws IOException {

    if (dbPath != null) {
      Path dbDir = dbPath.getParent();
      Path dbFile = dbPath.getFileName();
      if (dbDir != null && dbFile != null) {
        return getTransactionInfoFromDB(conf, dbDir, dbFile.toString(),
            definition);
      }
    }

    throw new IOException("Checkpoint " + dbPath + " does not have proper " +
        "DB location");
  }

  /**
   * Obtain Transaction info from DB.
   * @param dbDir path to DB
   */
  private static TransactionInfo getTransactionInfoFromDB(
      OzoneConfiguration tempConfig, Path dbDir, String dbName,
      DBDefinition definition)
      throws IOException {

    try (DBStore dbStore = DBStoreBuilder
        .newBuilder(tempConfig, definition, dbName, dbDir)
        .setOpenReadOnly(true)
        .setCreateCheckpointDirs(false)
        .setEnableRocksDbMetrics(false)
        .build()) {
      // Get the table name with TransactionInfo as the value. The transaction
      // info table name are different in SCM and SCM.

      // In case, a new table gets added where the value is TransactionInfo,
      // this logic may not work.

      Table<String, TransactionInfo> transactionInfoTable =
          getTransactionInfoTable(dbStore, definition);

      TransactionInfo transactionInfo =
          transactionInfoTable.get(TRANSACTION_INFO_KEY);

      if (transactionInfo == null) {
        throw new IOException("Failed to read TransactionInfo from DB " +
            definition.getName() + " at " + dbDir);
      }
      return transactionInfo;
    }
  }

  public static Table<String, TransactionInfo> getTransactionInfoTable(
      DBStore dbStore, DBDefinition definition) throws IOException {
    return (Table<String, TransactionInfo>)
        Streams.stream(definition.getColumnFamilies())
        .filter(t -> t.getValueType() == TransactionInfo.class).findFirst()
        .get().getTable(dbStore);
  }

  /**
   * Verify transaction info with provided lastAppliedIndex.
   *
   * If transaction info transaction Index is less than or equal to
   * lastAppliedIndex, return false, else return true.
   */
  public static boolean verifyTransactionInfo(TransactionInfo transactionInfo,
      long lastAppliedIndex, String leaderId, Path newDBlocation,
      Logger logger) {
    if (transactionInfo.getTransactionIndex() <= lastAppliedIndex) {
      logger.error("Failed to install checkpoint from the leader: {}"
              + ". The last applied index: {} is greater than or equal to the "
              + "checkpoint's applied index: {}. Deleting the downloaded "
              + "checkpoint {}", leaderId, lastAppliedIndex,
          transactionInfo.getTransactionIndex(), newDBlocation);
      try {
        FileUtils.deleteFully(newDBlocation);
      } catch (IOException e) {
        logger.error("Failed to fully delete the downloaded DB "
            + "checkpoint {} from SCM leader {}.", newDBlocation, leaderId, e);
      }
      return false;
    }
    return true;
  }

  public static File getMetaDir(DBDefinition definition,
      OzoneConfiguration configuration) {
    // Set metadata dirs.
    File metadataDir = definition.getDBLocation(configuration);

    if (metadataDir == null) {
      LOG.warn("{} is not configured. We recommend adding this setting. "
              + "Falling back to {} instead.",
          definition.getLocationConfigKey(), HddsConfigKeys.
              OZONE_METADATA_DIRS);
      metadataDir = getOzoneMetaDirPath(configuration);
    }
    return metadataDir;
  }

  /**
   * Scan the DB dir and return the existing files,
   * including omSnapshot files.
   *
   * @param db the file representing the DB to be scanned
   * @return the list of file names. If db not exist, will return empty list
   */
  public static List<String> getExistingFiles(File db) throws IOException {
    List<String> sstList = new ArrayList<>();
    if (!db.exists()) {
      return sstList;
    }
    // Walk the db dir and get all sst files including omSnapshot files.
    try (Stream<Path> files = Files.walk(db.toPath())) {
      sstList = files.filter(p -> p.toFile().isFile())
          .map(p -> p.getFileName().toString()).
              collect(Collectors.toList());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scanned files {} in {}.", sstList, db.getAbsolutePath());
      }
    }
    return sstList;
  }

  /**
   * Old Implementation i.e. when useInodeBasedCheckpoint = false,
   * the relative paths are sent in the toExcludeFile list to the leader OM.
   * @param db candidate OM Dir
   * @return a list of SST File paths relative to the DB.
   * @throws IOException in case of failure
   */
  public static List<String> getExistingSstFilesRelativeToDbDir(File db)
      throws IOException {
    List<String> sstList = new ArrayList<>();
    if (!db.exists()) {
      return sstList;
    }

    int truncateLength = db.toString().length() + 1;
    // Walk the db dir and get all sst files including omSnapshot files.
    try (Stream<Path> files = Files.walk(db.toPath())) {
      sstList =
          files.filter(path -> path.toString().endsWith(ROCKSDB_SST_SUFFIX)).
              map(p -> p.toString().substring(truncateLength)).
              collect(Collectors.toList());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scanned SST files {} in {}.", sstList, db.getAbsolutePath());
      }
    }
    return sstList;
  }

  /**
   * Retry forever until CA list matches expected count.
   * Fails fast on authentication exceptions.
   * @param task - task to get CA list.
   * @return CA list.
   */
  private static List<String> getCAListWithRetry(Callable<List<String>> task,
      long waitDuration) throws IOException {
    RetryPolicy retryPolicy = new RetryPolicy() {
      private final RetryPolicy defaultPolicy = RetryPolicies.retryForeverWithFixedSleep(
          waitDuration, TimeUnit.SECONDS);

      @Override
      public RetryAction shouldRetry(Exception e, int retries, int failovers, boolean isIdempotent) throws Exception {
        if (containsAccessControlException(e)) {
          LOG.warn("AccessControlException encountered during getCAList; failing fast without retry.");
          return new RetryAction(RetryAction.RetryDecision.FAIL);
        }
        return defaultPolicy.shouldRetry(e, retries, failovers, isIdempotent);
      }
    };

    RetriableTask<List<String>> retriableTask = new RetriableTask<>(retryPolicy, "getCAList", task);
    try {
      return retriableTask.call();
    } catch (Exception ex) {
      if (containsAccessControlException(ex)) {
        throw new AccessControlException();
      }
      throw new SCMSecurityException("Unable to obtain complete CA list", ex);
    }
  }

  private static boolean containsAccessControlException(Throwable e) {
    while (e != null) {
      if (e instanceof AccessControlException) {
        return true;
      }
      e = e.getCause();
    }
    return false;
  }

  private static List<String> waitForCACerts(
      final CheckedSupplier<List<String>, IOException> caCertListSupplier,
      int expectedCount) throws IOException {
    // TODO: If SCMs are bootstrapped later, then listCA need to be
    //  refetched if listCA size is less than scm ha config node list size.
    // For now when Client of SCM's are started we compare their node list
    // size and ca list size if it is as expected, we return the ca list.
    List<String> caCertPemList = caCertListSupplier.get();
    boolean caListUpToDate = caCertPemList.size() >= expectedCount;
    if (!caListUpToDate) {
      LOG.info("Expected CA list size {}, where as received CA List size " +
          "{}.", expectedCount, caCertPemList.size());
      throw new SCMSecurityException("Expected CA list size " + expectedCount
          + " is not matching actual count " + caCertPemList.size());
    }
    return caCertPemList;
  }

  /**
   * Build CA List in the format of X509Certificate.
   * If certificate client is null, obtain the list of CA using SCM
   * security client, else it uses certificate client.
   *
   * @return list of CA X509Certificates.
   */
  public static List<X509Certificate> buildCAX509List(ConfigurationSource conf) throws IOException {
    long waitDuration =
        conf.getTimeDuration(OZONE_SCM_CA_LIST_RETRY_INTERVAL,
            OZONE_SCM_CA_LIST_RETRY_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    Collection<String> scmNodes = HddsUtils.getSCMNodeIds(conf);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityProtocolClient =
        HddsServerUtil.getScmSecurityClient(conf);
    int expectedCount = scmNodes.size() + 1;
    if (scmNodes.size() > 1) {
      return OzoneSecurityUtil.convertToX509(getCAListWithRetry(() -> waitForCACerts(
          scmSecurityProtocolClient::listCACertificate,
          expectedCount), waitDuration));
    } else {
      return OzoneSecurityUtil.convertToX509(scmSecurityProtocolClient.listCACertificate());
    }
  }

}
