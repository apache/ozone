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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;

/**
 * Handles purging of keys from OM DB.
 */
public class OMKeyPurgeRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyPurgeRequest.class);

  public OMKeyPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<DeletedKeys> bucketDeletedKeysList = purgeKeysRequest
        .getDeletedKeysList();
    List<String> keysToBePurgedList = new ArrayList<>();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;


    for (DeletedKeys bucketWithDeleteKeys : bucketDeletedKeysList) {
      for (String deletedKey : bucketWithDeleteKeys.getKeysList()) {
        keysToBePurgedList.add(deletedKey);
      }
    }

    omClientResponse = new OMKeyPurgeResponse(omResponse.build(),
          keysToBePurgedList);
    addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
        omDoubleBufferHelper);

    return omClientResponse;
  }

  /**
   * Validates key purge requests.
   * We do not want to allow older clients to purge keys in buckets which use
   * non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.PurgeKeys
  )
  public static OMRequest blockPurgeKeysWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    List<DeletedKeys> bucketDeletedKeysList =
        req.getPurgeKeysRequest().getDeletedKeysList();

    // Add volume and bucket pairs to a hashset to avoid duplicate RPC
    // calls when querying bucket layout.
    HashSet<Pair<String, String>> volumeAndBucketList = new HashSet<>();
    for (DeletedKeys bucketWithDeleteKeys : bucketDeletedKeysList) {
      volumeAndBucketList.add(
          new ImmutablePair<>(bucketWithDeleteKeys.getVolumeName(),
              bucketWithDeleteKeys.getBucketName()));
    }

    // Validate each pair of volume and bucket.
    for (Pair<String, String> volAndBuck : volumeAndBucketList) {
      if (!ctx.getBucketLayout(volAndBuck.getLeft(), volAndBuck.getRight())
          .isLegacy()) {
        throw new OMException(
            "Client is attempting to purge one or more keys in a bucket" +
                " which uses non-LEGACY bucket layout features. Please " +
                "upgrade the client to a compatible version to perform this" +
                " operation.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
      }
    }
    return req;
  }
}
