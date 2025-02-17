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

package org.apache.hadoop.ozone.om.request.util;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Utility class to build OmResponse.
 */
public final class OmResponseUtil {

  private OmResponseUtil() {
  }

  /**
   * Get an initial OmResponse.Builder with proper request cmdType and traceID.
   * @param request OMRequest.
   * @return OMResponse builder.
   */
  public static OMResponse.Builder getOMResponseBuilder(OMRequest request) {
    return OMResponse.newBuilder()
        .setCmdType(request.getCmdType())
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setTraceID(request.getTraceID())
        .setSuccess(true);
  }
}
