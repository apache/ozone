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

package org.apache.hadoop.ozone.om.request.s3.metadata;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.SET_OBJECT_METADATA;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyInfoUpdateRequest;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetObjectMetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles set object metadata request: replaces the custom metadata and tags
 * of an existing key without touching its data. Used by the S3 gateway for a
 * CopyObject onto itself with metadata directive REPLACE.
 */
public class S3SetObjectMetadataRequest extends OMKeyInfoUpdateRequest {

  /**
   * System-managed metadata entries that a SetObjectMetadata request may never
   * change: they are always carried over from the existing key. The ETag only
   * reflects the object content, and dropping the GDPR entries would make a
   * GDPR key unreadable.
   */
  public static final Set<String> RESERVED_METADATA_KEYS = ImmutableSet.of(
      OzoneConsts.ETAG,
      OzoneConsts.GDPR_FLAG,
      OzoneConsts.GDPR_SECRET,
      OzoneConsts.GDPR_ALGORITHM,
      OzoneConsts.HSYNC_CLIENT_ID);

  private static final Logger LOG =
      LoggerFactory.getLogger(S3SetObjectMetadataRequest.class);

  public S3SetObjectMetadataRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  /**
   * Builds the replacement metadata map from the request metadata, with the
   * reserved system entries taken from the existing key instead of the request.
   */
  public static Map<String, String> replaceMetadata(Map<String, String> existingMetadata,
      Map<String, String> requestMetadata) {
    Map<String, String> newMetadata = new HashMap<>();
    for (Map.Entry<String, String> entry : requestMetadata.entrySet()) {
      if (!RESERVED_METADATA_KEYS.contains(entry.getKey())) {
        newMetadata.put(entry.getKey(), entry.getValue());
      }
    }
    for (String reservedKey : RESERVED_METADATA_KEYS) {
      String reservedValue = existingMetadata.get(reservedKey);
      if (reservedValue != null) {
        newMetadata.put(reservedKey, reservedValue);
      }
    }
    return newMetadata;
  }

  @Override
  @DisallowedUntilLayoutVersion(SET_OBJECT_METADATA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    return super.preExecute(ozoneManager);
  }

  @Override
  protected KeyArgs getKeyArgs(OMRequest omRequest) {
    return omRequest.getSetObjectMetadataRequest().getKeyArgs();
  }

  @Override
  protected OMRequest buildUpdatedRequest(OMRequest preExecutedRequest, KeyArgs resolvedKeyArgs)
      throws IOException {
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setSetObjectMetadataRequest(
            preExecutedRequest.getSetObjectMetadataRequest().toBuilder().setKeyArgs(resolvedKeyArgs))
        .build();
  }

  @Override
  protected OmKeyInfo.Builder applyUpdate(OmKeyInfo existingKeyInfo, KeyArgs keyArgs) {
    // Note: unlike object tagging, the key modification time is updated
    // because AWS CopyObject updates the object's LastModified.
    return existingKeyInfo.toBuilder()
        .setMetadata(replaceMetadata(existingKeyInfo.getMetadata(),
            KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList())))
        .setTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
        .setModificationTime(keyArgs.getModificationTime());
  }

  @Override
  protected boolean updatesModificationTime() {
    return true;
  }

  @Override
  protected void setSuccessResponse(OMResponse.Builder omResponse) {
    omResponse.setSetObjectMetadataResponse(SetObjectMetadataResponse.newBuilder());
  }

  @Override
  protected OMAction getAuditAction() {
    return OMAction.SET_OBJECT_METADATA;
  }

  @Override
  protected void incRequestMetric(OMMetrics omMetrics) {
    omMetrics.incNumSetObjectMetadata();
  }

  @Override
  protected void incFailureMetric(OMMetrics omMetrics) {
    omMetrics.incNumSetObjectMetadataFails();
  }

  @Override
  protected String getOperationName() {
    return "SetObjectMetadata";
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
