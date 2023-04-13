/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.function.SupplierWithIOException;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CA_LIST_RETRY_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CA_LIST_RETRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

/**
 * utility class used by SCM and OM for HA.
 */
public final class HAUtils {
  public static final Logger LOG = LoggerFactory.getLogger(HAUtils.class);

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
   * @throws IOException
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
   *
   * @return {@link ScmBlockLocationProtocol}
   * @throws IOException
   */
  public static ScmBlockLocationProtocol getScmBlockClient(
      OzoneConfiguration conf) {
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            new SCMBlockLocationFailoverProxyProvider(conf));
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

  /**
   * Replace the current DB with the new DB checkpoint.
   *
   * @param lastAppliedIndex the last applied index in the current SCM DB.
   * @param checkpointPath   path to the new DB checkpoint
   * @return location of backup of the original DB
   * @throws Exception
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
      FileUtils.moveDirectory(checkpointPath, oldDB.toPath());
      Files.deleteIfExists(markerFile);
    } catch (IOException e) {
      LOG.error("Failed to move downloaded DB checkpoint {} to metadata "
              + "directory {}. Resetting to original DB.", checkpointPath,
          oldDB.toPath());
      try {
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
      throws Exception {

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
   * @param tempConfig
   * @param dbDir path to DB
   * @return TransactionInfo
   * @throws Exception
   */
  private static TransactionInfo getTransactionInfoFromDB(
      OzoneConfiguration tempConfig, Path dbDir, String dbName,
      DBDefinition definition)
      throws Exception {

    try (DBStore dbStore = loadDB(tempConfig, dbDir.toFile(),
        dbName, definition)) {

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
    return Arrays.stream(definition.getColumnFamilies())
        .filter(t -> t.getValueType() == TransactionInfo.class).findFirst()
        .get().getTable(dbStore);
  }

  /**
   * Verify transaction info with provided lastAppliedIndex.
   *
   * If transaction info transaction Index is less than or equal to
   * lastAppliedIndex, return false, else return true.
   * @param transactionInfo
   * @param lastAppliedIndex
   * @param leaderId
   * @param newDBlocation
   * @return boolean
   */
  public static boolean verifyTransactionInfo(TransactionInfo transactionInfo,
      long lastAppliedIndex, String leaderId, Path newDBlocation,
      Logger logger) {
    if (transactionInfo.getTransactionIndex() <= lastAppliedIndex) {
      logger.error("Failed to install checkpoint from SCM leader: {}"
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

  public static DBStore loadDB(OzoneConfiguration configuration, File metaDir,
      String dbName, DBDefinition definition) throws IOException {
    RocksDBConfiguration rocksDBConfiguration =
        configuration.getObject(RocksDBConfiguration.class);
    DBStoreBuilder dbStoreBuilder =
        DBStoreBuilder.newBuilder(configuration, rocksDBConfiguration)
            .setName(dbName).setPath(Paths.get(metaDir.getPath()));
    // Add column family names and codecs.
    for (DBColumnFamilyDefinition columnFamily : definition
        .getColumnFamilies()) {

      dbStoreBuilder.addTable(columnFamily.getName());
      dbStoreBuilder
          .addCodec(columnFamily.getKeyType(), columnFamily.getKeyCodec());
      dbStoreBuilder
          .addCodec(columnFamily.getValueType(), columnFamily.getValueCodec());
    }
    return dbStoreBuilder.build();
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
   * Build CA list which need to be passed to client.
   *
   * If certificate client is null, obtain the list of CA using SCM security
   * client, else it uses certificate client.
   * @param certClient
   * @param configuration
   * @return list of CA
   * @throws IOException
   */
  public static List<String> buildCAList(CertificateClient certClient,
      ConfigurationSource configuration) throws IOException {
    long waitDuration =
        configuration.getTimeDuration(OZONE_SCM_CA_LIST_RETRY_INTERVAL,
            OZONE_SCM_CA_LIST_RETRY_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    if (certClient != null) {
      if (!SCMHAUtils.isSCMHAEnabled(configuration)) {
        return generateCAList(certClient);
      } else {
        Collection<String> scmNodes = SCMHAUtils.getSCMNodeIds(configuration);
        int expectedCount = scmNodes.size() + 1;
        if (scmNodes.size() > 1) {
          // First check if cert client has ca list initialized.
          // This is being done, when this method is called multiple times we
          // don't make call to SCM, we return from in-memory.
          List<String> caCertPemList = certClient.getCAList();
          if (caCertPemList != null && caCertPemList.size() == expectedCount) {
            return caCertPemList;
          }
          return getCAListWithRetry(() ->
                  waitForCACerts(certClient::updateCAList, expectedCount),
              waitDuration);
        } else {
          return generateCAList(certClient);
        }
      }
    } else {
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityProtocolClient =
          HddsServerUtil.getScmSecurityClient(configuration);
      if (!SCMHAUtils.isSCMHAEnabled(configuration)) {
        List<String> caCertPemList = new ArrayList<>();
        SCMGetCertResponseProto scmGetCertResponseProto =
            scmSecurityProtocolClient.getCACert();
        if (scmGetCertResponseProto.hasX509Certificate()) {
          caCertPemList.add(scmGetCertResponseProto.getX509Certificate());
        }
        if (scmGetCertResponseProto.hasX509RootCACertificate()) {
          caCertPemList.add(scmGetCertResponseProto.getX509RootCACertificate());
        }
        return caCertPemList;
      } else {
        Collection<String> scmNodes = SCMHAUtils.getSCMNodeIds(configuration);
        int expectedCount = scmNodes.size() + 1;
        if (scmNodes.size() > 1) {
          return getCAListWithRetry(() -> waitForCACerts(
              scmSecurityProtocolClient::listCACertificate,
              expectedCount), waitDuration);
        } else {
          return scmSecurityProtocolClient.listCACertificate();
        }
      }
    }
  }

  private static List<String> generateCAList(CertificateClient certClient)
      throws IOException {
    List<String> caCertPemList = new ArrayList<>();
    if (certClient.getRootCACertificate() != null) {
      caCertPemList.add(CertificateCodec.getPEMEncodedString(
          certClient.getRootCACertificate()));
    }
    if (certClient.getCACertificate() != null) {
      caCertPemList.add(CertificateCodec.getPEMEncodedString(
          certClient.getCACertificate()));
    }
    return caCertPemList;
  }

  /**
   * Retry for ever until CA list matches expected count.
   * @param task - task to get CA list.
   * @return CA list.
   */
  private static List<String> getCAListWithRetry(Callable<List<String>> task,
      long waitDuration) throws IOException {
    RetryPolicy retryPolicy = RetryPolicies.retryForeverWithFixedSleep(
        waitDuration, TimeUnit.SECONDS);
    RetriableTask<List<String>> retriableTask =
        new RetriableTask<>(retryPolicy, "getCAList", task);
    try {
      return retriableTask.call();
    } catch (Exception ex) {
      throw new SCMSecurityException("Unable to obtain complete CA " +
          "list", ex);
    }
  }

  private static List<String> waitForCACerts(
      final SupplierWithIOException<List<String>> applyFunction,
      int expectedCount) throws IOException {
    // TODO: If SCMs are bootstrapped later, then listCA need to be
    //  refetched if listCA size is less than scm ha config node list size.
    // For now when Client of SCM's are started we compare their node list
    // size and ca list size if it is as expected, we return the ca list.
    List<String> caCertPemList = applyFunction.get();
    boolean caListUpToDate = caCertPemList.size() == expectedCount;
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
   * @param certClient
   * @param conf
   * @return list of CA X509Certificates.
   * @throws IOException
   */
  public static List<X509Certificate> buildCAX509List(
      CertificateClient certClient,
      ConfigurationSource conf) throws IOException {
    if (certClient != null) {
      // Do this here to avoid extra conversion of X509 to pem and again to
      // X509 by buildCAList.
      if (!SCMHAUtils.isSCMHAEnabled(conf)) {
        List<X509Certificate> x509Certificates = new ArrayList<>();
        if (certClient.getRootCACertificate() != null) {
          x509Certificates.add(certClient.getRootCACertificate());
        }
        x509Certificates.add(certClient.getCACertificate());
        return x509Certificates;
      }
    }
    List<String> pemEncodedCerts = HAUtils.buildCAList(certClient, conf);
    return OzoneSecurityUtil.convertToX509(pemEncodedCerts);
  }

}
