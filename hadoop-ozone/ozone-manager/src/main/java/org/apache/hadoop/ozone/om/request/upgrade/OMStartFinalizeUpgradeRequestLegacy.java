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

package org.apache.hadoop.ozone.om.request.upgrade;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.FinalizeUpgrade;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.UpgradeFinalizationStatus;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Handles the finalizeUpgrade request sent by the old CLI ({@code ozone admin om finalizeupgrade}).
 * It initiates the same asynchronous finalization flow as {@link OMStartFinalizeUpgradeRequest} and
 * returns {@code STARTING_FINALIZATION} to be compatible with old clients.
 * The old CLI then polls finalization progress until done.
 */
public class OMStartFinalizeUpgradeRequestLegacy extends OMFinalizeUpgradeRequestBase {

  public OMStartFinalizeUpgradeRequestLegacy(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  protected boolean isForce() {
    // The old request has no force field.
    return false;
  }

  @Override
  protected void setResponseBody(OMResponse.Builder builder, OzoneManager ozoneManager) {
    builder.setCmdType(FinalizeUpgrade);
    UpgradeFinalizationStatus status = UpgradeFinalizationStatus.newBuilder()
        .setStatus(UpgradeFinalizationStatus.Status.STARTING_FINALIZATION)
        .build();
    builder.setFinalizeUpgradeResponse(FinalizeUpgradeResponse.newBuilder()
        .setStatus(status)
        .build());
  }
}
