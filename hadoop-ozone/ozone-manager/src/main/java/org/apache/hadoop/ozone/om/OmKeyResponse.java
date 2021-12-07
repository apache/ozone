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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * OmKeyResponse.
 */
public abstract class OmKeyResponse extends OMClientResponse {

  private BucketLayout bucketLayout;

  private static final Logger LOG =
      LoggerFactory.getLogger(OmKeyResponse.class);

  public OmKeyResponse(OzoneManagerProtocolProtos.OMResponse omResponse,
      BucketLayout bucketLayoutArg) {
    super(omResponse);
    this.bucketLayout = bucketLayoutArg;
  }

  public OmKeyResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
    this.bucketLayout = BucketLayout.DEFAULT;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  protected BucketLayout getBucketLayout(OMMetadataManager omMetadataManager,
      String volName, String buckName) {
    if (omMetadataManager == null) {
      return BucketLayout.DEFAULT;
    }
    String buckKey = omMetadataManager.getBucketKey(volName, buckName);
    try {
      OmBucketInfo buckInfo = omMetadataManager.getBucketTable().get(buckKey);
      return buckInfo.getBucketLayout();
    } catch (IOException e) {
      LOG.error("Cannot find the key: " + buckKey, e);
      // Todo: Handle Exception
    }
    return BucketLayout.DEFAULT;
  }
}
