/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.ratis.execution;

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.ozone.om.ratis.execution.request.ExecutionContext;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMRequestExecutor;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * request processing information.
 */
public final class RequestContext {
  private OMRequest request;
  private OMRequestExecutor requestExecutor;
  private OMResponse response;
  private CompletableFuture<OMResponse> future;
  private String uuidClientId;
  private long callId;
  private ExecutionContext executionContext;

  public RequestContext() {
  }

  public OMRequest getRequest() {
    return request;
  }

  public void setRequest(OMRequest request) {
    this.request = request;
  }

  public OMResponse getResponse() {
    return response;
  }

  public void setResponse(OMResponse response) {
    this.response = response;
  }

  public CompletableFuture<OMResponse> getFuture() {
    return future;
  }

  public void setFuture(CompletableFuture<OMResponse> future) {
    this.future = future;
  }

  public void setRequestExecutor(OMRequestExecutor requestBase) {
    this.requestExecutor = requestBase;
  }
  public OMRequestExecutor getRequestExecutor() {
    return requestExecutor;
  }

  public void setUuidClientId(String clientId) {
    this.uuidClientId = clientId;
  }

  public String getUuidClientId() {
    return uuidClientId;
  }

  public void setCallId(long callId) {
    this.callId = callId;
  }

  public long getCallId() {
    return callId;
  }

  public ExecutionContext getExecutionContext() {
    return executionContext;
  }

  public void setExecutionContext(ExecutionContext executionContext) {
    this.executionContext = executionContext;
  }
}
