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

import org.apache.hadoop.hdds.HddsConfigKeys;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

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

  public static ScmInfo getScmInfo(OzoneConfiguration conf)
      throws IOException {
    try {
      return getScmBlockClient(conf).getScmInfo();
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
      OzoneConfiguration conf) throws IOException {
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            new SCMBlockLocationFailoverProxyProvider(conf));
    return TracingUtil
        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
            conf);
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
      Files.move(checkpointPath, oldDB.toPath());
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
    DBStore dbStore = loadDB(tempConfig, dbDir.toFile(),
        dbName, definition);

    // Get the table name with TransactionInfo as the value. The transaction
    // info table name are different in SCM and SCM.

    // In case, a new table gets added where the value is TransactionInfo, this
    // logic may not work.


    Table<String, TransactionInfo> transactionInfoTable =
        getTransactionInfoTable(dbStore, definition);

    TransactionInfo transactionInfo =
        transactionInfoTable.get(TRANSACTION_INFO_KEY);
    dbStore.close();

    if (transactionInfo == null) {
      throw new IOException("Failed to read TransactionInfo from DB " +
          definition.getName() + " at " + dbDir);
    }
    return transactionInfo;
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
   * Unwrap exception to check if it is some kind of access control problem.
   * {@link AccessControlException}
   */
  public static boolean isAccessControlException(Exception ex) {
    if (ex instanceof ServiceException) {
      Throwable t = ex.getCause();
      if (t instanceof RemoteException) {
        t = ((RemoteException) t).unwrapRemoteException();
      }
      while (t != null) {
        if (t instanceof AccessControlException) {
          return true;
        }
        t = t.getCause();
      }
    }
    return false;
  }

  public static void checkSecurityAndSCMHAEnabled(OzoneConfiguration conf) {
    boolean enable =
        conf.getBoolean(ScmConfigKeys.OZONE_SCM_HA_SECURITY_SUPPORTED,
            ScmConfigKeys.OZONE_SCM_HA_SECURITY_SUPPORTED_DEFAULT);
    if (OzoneSecurityUtil.isSecurityEnabled(conf) && !enable) {
      List<SCMNodeInfo> scmNodeInfo = SCMNodeInfo.buildNodeInfo(conf);
      if (scmNodeInfo.size() > 1) {
        System.err.println("Ozone Services cannot be started on a secure SCM " +
            "HA enabled cluster");
        System.exit(1);
      }
    }
  }
}
