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

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;

/**
 * The OM client request operations used by callers outside the request class.
 * {@link org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils#createClientRequest} returns this type so that
 * the request can be wrapped in a tracing proxy. See {@link OMClientRequest} for the method contracts.
 */
public interface IOMClientRequest extends RequestAuditor {

  OMRequest preExecute(OzoneManager ozoneManager) throws IOException;

  void handleRequestFailure(OzoneManager ozoneManager);

  OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context);

  OMAuditLogger.Builder getAuditBuilder();

  OMRequest getOmRequest();

  UserInfo getUserInfo() throws IOException;
}
