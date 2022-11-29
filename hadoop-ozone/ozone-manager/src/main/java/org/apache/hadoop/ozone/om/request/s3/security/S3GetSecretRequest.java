/**
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

package org.apache.hadoop.ozone.om.request.s3.security;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;

/**
 * Handles GetS3Secret request.
 */
public class S3GetSecretRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GetSecretRequest.class);

  public S3GetSecretRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    final GetS3SecretRequest s3GetSecretRequest =
        getOmRequest().getGetS3SecretRequest();

    // The proto field kerberosID is effectively accessId w/ Multi-Tenancy
    //
    // But it is still named kerberosID because kerberosID == accessId before
    // multi-tenancy feature is implemented. And renaming proto field fails the
    // protolock check.
    final String accessId = s3GetSecretRequest.getKerberosID();

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    // Permission check
    S3SecretRequestHelper.checkAccessIdSecretOpPermission(
        ozoneManager, ugi, accessId);

    // Client issues GetS3Secret request, when received by OM leader
    // it will generate s3Secret. Original GetS3Secret request is
    // converted to UpdateGetS3Secret request with the generated token
    // information. This updated request will be submitted to Ratis. In this
    // way S3Secret created by leader, will be replicated across all
    // OMs. With this approach, original GetS3Secret request from
    // client does not need any proto changes.
    OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setUserInfo(getUserInfo())
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    // createIfNotExist defaults to true if not specified.
    boolean createIfNotExist = !s3GetSecretRequest.hasCreateIfNotExist()
            || s3GetSecretRequest.getCreateIfNotExist();

    // Recompose GetS3SecretRequest just in case createIfNotExist is missing
    final GetS3SecretRequest newGetS3SecretRequest =
            GetS3SecretRequest.newBuilder()
                    .setKerberosID(accessId)  // See Note 1 above
                    .setCreateIfNotExist(createIfNotExist)
                    .build();
    omRequest.setGetS3SecretRequest(newGetS3SecretRequest);

    // When createIfNotExist is true, pass UpdateGetS3SecretRequest message;
    // otherwise, just use GetS3SecretRequest message.
    if (createIfNotExist) {
      // Generate secret here because this will be written to DB only when
      // createIfNotExist is true and accessId entry doesn't exist in DB.
      String s3Secret = DigestUtils.sha256Hex(OmUtils.getSHADigest());

      final UpdateGetS3SecretRequest updateGetS3SecretRequest =
              UpdateGetS3SecretRequest.newBuilder()
                      .setKerberosID(accessId)  // See Note 1 above
                      .setAwsSecret(s3Secret)
                      .build();

      omRequest.setUpdateGetS3SecretRequest(updateGetS3SecretRequest);
    }

    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }

    return omRequest.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    boolean acquiredLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    final GetS3SecretRequest getS3SecretRequest =
            getOmRequest().getGetS3SecretRequest();
    assert (getS3SecretRequest.hasCreateIfNotExist());
    final boolean createIfNotExist = getS3SecretRequest.getCreateIfNotExist();
    // See Note 1 above
    final String accessId = getS3SecretRequest.getKerberosID();
    String awsSecret = null;
    if (createIfNotExist) {
      final UpdateGetS3SecretRequest updateGetS3SecretRequest =
              getOmRequest().getUpdateGetS3SecretRequest();
      awsSecret = updateGetS3SecretRequest.getAwsSecret();
      assert (accessId.equals(updateGetS3SecretRequest.getKerberosID()));
    }

    try {
      acquiredLock = omMetadataManager.getLock()
          .acquireWriteLock(S3_SECRET_LOCK, accessId);

      final S3SecretValue assignS3SecretValue;
      final S3SecretValue s3SecretValue =
          omMetadataManager.getS3SecretTable().get(accessId);

      if (s3SecretValue == null) {
        // Not found in S3SecretTable.
        if (createIfNotExist) {
          // Add new entry in this case
          assignS3SecretValue = new S3SecretValue(accessId, awsSecret);
          // Add cache entry first.
          omMetadataManager.getS3SecretTable().addCacheEntry(
                  new CacheKey<>(accessId),
                  new CacheValue<>(Optional.of(assignS3SecretValue),
                          transactionLogIndex));
        } else {
          assignS3SecretValue = null;
        }
      } else {
        // Found in S3SecretTable.
        awsSecret = s3SecretValue.getAwsSecret();
        assignS3SecretValue = null;
      }

      // Throw ACCESS_ID_NOT_FOUND to the client if accessId doesn't exist
      //  when createIfNotExist is false.
      if (awsSecret == null) {
        assert (!createIfNotExist);
        throw new OMException("accessId '" + accessId + "' doesn't exist",
                OMException.ResultCodes.ACCESS_ID_NOT_FOUND);
      }

      // Compose response
      final GetS3SecretResponse.Builder getS3SecretResponse =
              GetS3SecretResponse.newBuilder().setS3Secret(
                      S3Secret.newBuilder()
                              .setAwsSecret(awsSecret)
                              .setKerberosID(accessId)  // See Note 1 above
              );
      // If entry exists or createIfNotExist is false, assignS3SecretValue
      // will be null, so we won't write or overwrite the entry.
      omClientResponse = new S3GetSecretResponse(assignS3SecretValue,
              omResponse.setGetS3SecretResponse(getS3SecretResponse).build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3GetSecretResponse(null,
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK,
            accessId);
      }
    }

    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_GETSECRET_USER, accessId);

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.GET_S3_SECRET, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Success: GetSecret for accessKey '{}', createIfNotExist '{}'",
              accessId, createIfNotExist);
    } else {
      LOG.error("Failed to GetSecret for accessKey '{}', createIfNotExist " +
                      "'{}': {}", accessId, createIfNotExist, exception);
    }
    return omClientResponse;
  }

}
