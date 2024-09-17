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
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * request processing information.
 */
public final class RequestContext {
  private OMRequest request;
  private OMClientRequest clientRequest;
  private OMResponse response;
  private TermIndex index;
  private CompletableFuture<OMResponse> future;
  private OzoneManagerProtocolProtos.PersistDbRequest.Builder nextRequest;

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

  public TermIndex getIndex() {
    return index;
  }

  public void setIndex(TermIndex index) {
    this.index = index;
  }

  public CompletableFuture<OMResponse> getFuture() {
    return future;
  }

  public void setFuture(CompletableFuture<OMResponse> future) {
    this.future = future;
  }

  public OzoneManagerProtocolProtos.PersistDbRequest.Builder getNextRequest() {
    return nextRequest;
  }

  public void setNextRequest(OzoneManagerProtocolProtos.PersistDbRequest.Builder nextRequest) {
    this.nextRequest = nextRequest;
  }

  public OMClientRequest getClientRequest() {
    return clientRequest;
  }

  public void setClientRequest(OMClientRequest clientRequest) {
    this.clientRequest = clientRequest;
  }
}
