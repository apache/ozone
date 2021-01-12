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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis.utils;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.OMTransactionInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketAddAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketSetAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixSetAclRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetOwnerRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetQuotaRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeAddAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeSetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.util.FileUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TRANSACTION_INFO_TABLE;

/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneManagerRatisUtils {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerRatisUtils.class);

  private OzoneManagerRatisUtils() {
  }

  public static OMClientRequest getRequest(OzoneManager om,
                                           OMRequest omRequest) {
    Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case AddAcl:
    case RemoveAcl:
    case SetAcl:
      return getOMAclRequest(om, omRequest);
    case SetVolumeProperty:
      return getVolumeSetPropertyRequest(om, omRequest);
    default:
      Class<? extends OMClientRequest> requestClass =
          om.getVersionManager()
              .getHandler(omRequest.getCmdType().name());
      return getClientRequest(requestClass, omRequest);
    }
  }

  private static OMClientRequest getClientRequest(Class<?
      extends OMClientRequest> requestClass, OMRequest omRequest) {
    try {
      return requestClass.getDeclaredConstructor(OMRequest.class)
          .newInstance(omRequest);
    } catch (Exception ex) {
      LOG.error("Unable to get request handler for '{}', current layout " +
              "version = {}, request factory returned '{}'",
          omRequest.getCmdType(),
          omRequest.getLayoutVersion().getVersion(),
          requestClass.getSimpleName());
    }
    throw new IllegalStateException("Unrecognized write command " +
        "type request : " + omRequest.getCmdType());
  }

  public static OMClientRequest getOMAclRequest(OzoneManager om,
                                                OMRequest omRequest) {
    Type cmdType = omRequest.getCmdType();
    String requestType = null;
    if (Type.AddAcl == cmdType) {
      ObjectType type = omRequest.getAddAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        requestType = OMVolumeAddAclRequest.getRequestType();
      } else if (ObjectType.BUCKET == type) {
        requestType = OMBucketAddAclRequest.getRequestType();
      } else if (ObjectType.KEY == type) {
        requestType = OMKeyAddAclRequest.getRequestType();
      } else {
        requestType = OMPrefixAddAclRequest.getRequestType();
      }
    } else if (Type.RemoveAcl == cmdType) {
      ObjectType type = omRequest.getRemoveAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        requestType = OMVolumeRemoveAclRequest.getRequestType();
      } else if (ObjectType.BUCKET == type) {
        requestType = OMBucketRemoveAclRequest.getRequestType();
      } else if (ObjectType.KEY == type) {
        requestType = OMKeyRemoveAclRequest.getRequestType();
      } else {
        requestType = OMPrefixRemoveAclRequest.getRequestType();
      }
    } else {
      ObjectType type = omRequest.getSetAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        requestType = OMVolumeSetAclRequest.getRequestType();
      } else if (ObjectType.BUCKET == type) {
        requestType = OMBucketSetAclRequest.getRequestType();
      } else if (ObjectType.KEY == type) {
        requestType = OMKeySetAclRequest.getRequestType();
      } else {
        requestType = OMPrefixSetAclRequest.getRequestType();
      }
    }
    Class<? extends OMClientRequest> requestClass =
        om.getVersionManager().getHandler(requestType);
    return getClientRequest(requestClass, omRequest);
  }

  public static OMClientRequest getVolumeSetPropertyRequest(
      OzoneManager om, OMRequest omRequest) {
    boolean hasQuota = omRequest.getSetVolumePropertyRequest()
        .hasQuotaInBytes();
    boolean hasOwner = omRequest.getSetVolumePropertyRequest().hasOwnerName();
    Preconditions.checkState(hasOwner || hasQuota,
        "Either Quota or owner " +
            "should be set in the SetVolumeProperty request");
    Preconditions.checkState(!(hasOwner && hasQuota),
        "Either Quota or " +
            "owner should be set in the SetVolumeProperty request. Should not "
            + "set both");

    String requestType = hasQuota ? OMVolumeSetQuotaRequest.getRequestType() :
        OMVolumeSetOwnerRequest.getRequestType();
    Class<? extends OMClientRequest> requestClass =
        om.getVersionManager().getHandler(requestType);
    return getClientRequest(requestClass, omRequest);
  }


  /**
   * Convert exception result to {@link OzoneManagerProtocolProtos.Status}.
   * @param exception
   * @return OzoneManagerProtocolProtos.Status
   */
  public static Status exceptionToResponseStatus(IOException exception) {
    if (exception instanceof OMException) {
      return Status.values()[((OMException) exception).getResult().ordinal()];
    } else {
      // Doing this here, because when DB error happens we need to return
      // correct error code, so that in applyTransaction we can
      // completeExceptionally for DB errors.

      // Currently Table API's are in hdds-common, which are used by SCM, OM
      // currently. So, they return IOException with setting cause to
      // RocksDBException. So, here that is the reason for the instanceof
      // check RocksDBException.
      if (exception.getCause() != null
          && exception.getCause() instanceof RocksDBException) {
        return Status.METADATA_ERROR;
      } else {
        return Status.INTERNAL_ERROR;
      }
    }
  }

  /**
   * Obtain OMTransactionInfo from Checkpoint.
   */
  public static OMTransactionInfo getTrxnInfoFromCheckpoint(
      OzoneConfiguration conf, Path dbPath) throws Exception {

    if (dbPath != null) {
      Path dbDir = dbPath.getParent();
      Path dbFile = dbPath.getFileName();
      if (dbDir != null && dbFile != null) {
        return getTransactionInfoFromDB(conf, dbDir, dbFile.toString());
      }
    }
    
    throw new IOException("Checkpoint " + dbPath + " does not have proper " +
        "DB location");
  }

  /**
   * Obtain Transaction info from DB.
   * @param tempConfig
   * @param dbDir path to DB
   * @return OMTransactionInfo
   * @throws Exception
   */
  private static OMTransactionInfo getTransactionInfoFromDB(
      OzoneConfiguration tempConfig, Path dbDir, String dbName)
      throws Exception {
    DBStore dbStore = OmMetadataManagerImpl.loadDB(tempConfig, dbDir.toFile(),
        dbName);

    Table<String, OMTransactionInfo> transactionInfoTable =
        dbStore.getTable(TRANSACTION_INFO_TABLE,
            String.class, OMTransactionInfo.class);

    OMTransactionInfo omTransactionInfo =
        transactionInfoTable.get(TRANSACTION_INFO_KEY);
    dbStore.close();

    if (omTransactionInfo == null) {
      throw new IOException("Failed to read OMTransactionInfo from DB " +
          dbName + " at " + dbDir);
    }
    return omTransactionInfo;
  }

  /**
   * Verify transaction info with provided lastAppliedIndex.
   *
   * If transaction info transaction Index is less than or equal to
   * lastAppliedIndex, return false, else return true.
   * @param omTransactionInfo
   * @param lastAppliedIndex
   * @param leaderId
   * @param newDBlocation
   * @return boolean
   */
  public static boolean verifyTransactionInfo(
      OMTransactionInfo omTransactionInfo,
      long lastAppliedIndex,
      String leaderId, Path newDBlocation) {
    if (omTransactionInfo.getTransactionIndex() <= lastAppliedIndex) {
      OzoneManager.LOG.error("Failed to install checkpoint from OM leader: {}" +
              ". The last applied index: {} is greater than or equal to the " +
              "checkpoint's applied index: {}. Deleting the downloaded " +
              "checkpoint {}", leaderId, lastAppliedIndex,
          omTransactionInfo.getTransactionIndex(), newDBlocation);
      try {
        FileUtils.deleteFully(newDBlocation);
      } catch (IOException e) {
        OzoneManager.LOG.error("Failed to fully delete the downloaded DB " +
            "checkpoint {} from OM leader {}.", newDBlocation, leaderId, e);
      }
      return false;
    }

    return true;
  }
}
