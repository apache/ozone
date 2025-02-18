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

package org.apache.hadoop.ozone.om.request.s3.security;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    final UserGroupInformation ugi =
        S3SecretRequestHelper.getOrCreateUgi(accessId);
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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    Exception exception = null;

    final GetS3SecretRequest getS3SecretRequest =
            getOmRequest().getGetS3SecretRequest();
    assert (getS3SecretRequest.hasCreateIfNotExist());
    final boolean createIfNotExist = getS3SecretRequest.getCreateIfNotExist();
    // See Note 1 above
    final String accessId = getS3SecretRequest.getKerberosID();
    AtomicReference<String> awsSecret = new AtomicReference<>();
    if (createIfNotExist) {
      final UpdateGetS3SecretRequest updateGetS3SecretRequest =
              getOmRequest().getUpdateGetS3SecretRequest();
      awsSecret.set(updateGetS3SecretRequest.getAwsSecret());
      assert (accessId.equals(updateGetS3SecretRequest.getKerberosID()));
    }

    try {
      omClientResponse = ozoneManager.getS3SecretManager()
          .doUnderLock(accessId, s3SecretManager -> {
            final S3SecretValue assignS3SecretValue;
            S3SecretValue s3SecretValue = s3SecretManager.getSecret(accessId);

            if (s3SecretValue == null) {
              // Not found in S3SecretTable.
              if (createIfNotExist) {
                // Add new entry in this case
                assignS3SecretValue = S3SecretValue.of(accessId, awsSecret.get(), context.getIndex());
                // Add cache entry first.
                s3SecretManager.updateCache(accessId, assignS3SecretValue);
              } else {
                assignS3SecretValue = null;
              }
            } else {
              final OMMultiTenantManager multiTenantManager =
                  ozoneManager.getMultiTenantManager();
              if (multiTenantManager == null ||
                  !multiTenantManager.getTenantForAccessID(accessId)
                      .isPresent()) {
                // Access Id is not assigned to any tenant and
                // Secret is found in S3SecretTable. No secret is returned.
                throw new OMException("Secret for '" + accessId +
                    "' already exists", OMException.ResultCodes.
                    S3_SECRET_ALREADY_EXISTS);
              }
              // For tenant getsecret, secret is always returned
              awsSecret.set(s3SecretValue.getAwsSecret());
              assignS3SecretValue = null;
            }

            // Throw ACCESS_ID_NOT_FOUND to the client if accessId doesn't exist
            //  when createIfNotExist is false.
            if (awsSecret.get() == null) {
              assert (!createIfNotExist);
              throw new OMException("accessId '" + accessId + "' doesn't exist",
                  OMException.ResultCodes.ACCESS_ID_NOT_FOUND);
            }

            if (assignS3SecretValue != null && !s3SecretManager.isBatchSupported()) {
              // A storage that does not support batch writing is likely to be a
              // third-party secret storage that might throw an exception on write.
              // In the case of the exception the request will fail.
              s3SecretManager.storeSecret(assignS3SecretValue.getKerberosID(),
                                          assignS3SecretValue);
            }

            // Compose response
            final GetS3SecretResponse.Builder getS3SecretResponse =
                GetS3SecretResponse.newBuilder().setS3Secret(
                    S3Secret.newBuilder()
                        .setAwsSecret(awsSecret.get())
                        .setKerberosID(accessId)  // See Note 1 above
                );
            // If entry exists or createIfNotExist is false, assignS3SecretValue
            // will be null, so we won't write or overwrite the entry.
            return new S3GetSecretResponse(assignS3SecretValue,
                s3SecretManager,
                omResponse.setGetS3SecretResponse(getS3SecretResponse).build());
          });


    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new S3GetSecretResponse(null,
          ozoneManager.getS3SecretManager(),
          createErrorOMResponse(omResponse, exception));
    }

    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_GETSECRET_USER, accessId);

    // audit log
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
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
