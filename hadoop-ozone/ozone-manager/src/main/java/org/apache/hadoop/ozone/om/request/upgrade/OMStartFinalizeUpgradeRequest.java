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

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.StartFinalizeUpgrade;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Starts the cluster upgrade finalization process via {@code ozone admin upgrade finalize}.
 */
public class OMStartFinalizeUpgradeRequest extends OMFinalizeUpgradeRequestBase {

  public OMStartFinalizeUpgradeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  protected boolean isForce() {
    return getOmRequest().getStartFinalizeUpgradeRequest().getForce();
  }

  @Override
  protected void setResponseBody(OMResponse.Builder builder, OzoneManager ozoneManager) {
    builder.setCmdType(StartFinalizeUpgrade);
    builder.setStartFinalizeUpgradeResponse(
        OzoneManagerProtocolProtos.StartFinalizeUpgradeResponse.newBuilder().build());
  }
}
