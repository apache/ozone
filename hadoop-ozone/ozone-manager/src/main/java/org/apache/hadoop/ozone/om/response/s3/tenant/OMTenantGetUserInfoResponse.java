/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.response.s3.tenant;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantUserInfo;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**
 * Response for TenantGetUserInfo.
 */
public class OMTenantGetUserInfoResponse extends OMClientResponse {

  private String userPrincipal;
  private List<TenantUserInfo> tenantUserInfoList;

  public OMTenantGetUserInfoResponse(@Nonnull OMResponse omResponse,
      @Nonnull String userPrincipal,
      @Nonnull List<TenantUserInfo> tenantUserInfoList) {
    super(omResponse);
    this.userPrincipal = userPrincipal;
    this.tenantUserInfoList = tenantUserInfoList;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTenantGetUserInfoResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    // No-Op
  }

  @VisibleForTesting
  public String getUserPrincipal() {
    return userPrincipal;
  }

  @VisibleForTesting
  public List<TenantUserInfo> getTenantUserInfoList() {
    return tenantUserInfoList;
  }

}
