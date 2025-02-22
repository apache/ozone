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

package org.apache.hadoop.ozone.om.response.util;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for EchoRPC request (write).
 */
@CleanupTableInfo
public class OMEchoRPCWriteResponse extends OMClientResponse {

  public OMEchoRPCWriteResponse(OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
                              BatchOperation batchOperation)
      throws IOException {
    // Overrides the addToDBBatch method to prevent a db/cache update.
    // The purpose of this override is to facilitate isolated benchmarking of
    // Ratis without extensive backend modifications.
    return;
  }
}
