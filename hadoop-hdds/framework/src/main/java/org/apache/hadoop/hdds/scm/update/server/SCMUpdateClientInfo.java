/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.server;

import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateResponse;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;

import java.util.UUID;

/**
 * Wrapper class for scm update client on server side.
 */
public class SCMUpdateClientInfo {
  private StreamObserver<UpdateResponse> responseObserver;
  private UUID clientId;

  public SCMUpdateClientInfo(UUID clientId) {
    this(clientId, null);
  }

  public SCMUpdateClientInfo(UUID clientId,
      StreamObserver<UpdateResponse> responseObserver) {
    this.clientId = clientId;
    this.responseObserver = responseObserver;
  }

  public UUID getClientId() {
    return clientId;
  }

  public static SCMUpdateServiceProtos.ClientId toClientIdProto(UUID uuid) {
    return SCMUpdateServiceProtos.ClientId.newBuilder()
        .setLsb(uuid.getLeastSignificantBits())
        .setMsb(uuid.getMostSignificantBits()).build();
  }

  public static UUID fromClientIdProto(
      SCMUpdateServiceProtos.ClientId clientId) {
    return new UUID(clientId.getMsb(), clientId.getLsb());
  }

  public StreamObserver<UpdateResponse> getResponseObserver() {
    return responseObserver;
  }

  public void setResponseObserver(
      StreamObserver<UpdateResponse> responseObserver) {
    this.responseObserver = responseObserver;
  }
}
