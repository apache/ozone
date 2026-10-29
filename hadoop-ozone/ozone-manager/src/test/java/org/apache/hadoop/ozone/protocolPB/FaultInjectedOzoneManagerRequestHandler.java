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

package org.apache.hadoop.ozone.protocolPB;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * {@link RequestHandler} decorator that calls {@link FaultInjector#pause()} before each write request.
 */
public class FaultInjectedOzoneManagerRequestHandler implements RequestHandler {
  private final RequestHandler delegate;
  private final FaultInjector injector;

  public FaultInjectedOzoneManagerRequestHandler(RequestHandler delegate, FaultInjector injector) {
    this.delegate = delegate;
    this.injector = injector;
  }

  @Override
  public OMResponse handleReadRequest(OMRequest request) {
    return delegate.handleReadRequest(request);
  }

  @Override
  public void validateRequest(OMRequest omRequest) throws OMException {
    delegate.validateRequest(omRequest);
  }

  @Override
  public OMClientResponse handleWriteRequestImpl(OMRequest omRequest, ExecutionContext context) throws IOException {
    injector.pause();
    return delegate.handleWriteRequestImpl(omRequest, context);
  }
}
