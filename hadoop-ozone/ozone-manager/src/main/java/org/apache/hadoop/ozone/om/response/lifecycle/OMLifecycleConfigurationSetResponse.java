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

package org.apache.hadoop.ozone.om.response.lifecycle;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.LIFECYCLE_CONFIGURATION_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for SetLifecycleConfiguration request.
 */
@CleanupTableInfo(cleanupTables = {LIFECYCLE_CONFIGURATION_TABLE})
public class OMLifecycleConfigurationSetResponse extends OMClientResponse  {

  private final OmLifecycleConfiguration omLifecycleConfiguration;

  public OMLifecycleConfigurationSetResponse(
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.omLifecycleConfiguration = null;
  }

  public OMLifecycleConfigurationSetResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmLifecycleConfiguration omLifecycleConfiguration) {
    super(omResponse);
    this.omLifecycleConfiguration = omLifecycleConfiguration;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbLifecycleKey = omMetadataManager.getBucketKey(
        omLifecycleConfiguration.getVolume(),
        omLifecycleConfiguration.getBucket());

    omMetadataManager.getLifecycleConfigurationTable().putWithBatch(
        batchOperation, dbLifecycleKey, omLifecycleConfiguration);
  }

  public OmLifecycleConfiguration getOmLifecycleConfiguration() {
    return omLifecycleConfiguration;
  }
}
