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

import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceGrpc;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.SubscribeRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.SubscribeResponse;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.Type;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateResponse;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UnsubscribeRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UnsubscribeResponse;

import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.update.client.CRLStore;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Impl class for SCM update service.
 * Allows client to subscribe and bi-directional streaming update with server.
 * Currently used for CRL udpate between SCM and OM.
 * Can be extended to update SCM pipeline/container change in future.
 */
public class SCMUpdateServiceImpl extends
    SCMUpdateServiceGrpc.SCMUpdateServiceImplBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMUpdateServiceImpl.class);

  private SCMUpdateClientManager clientManager;

  public SCMUpdateServiceImpl(CRLStore crlStore) {
    clientManager = new SCMUpdateClientManager();
    clientManager.registerHandler(new SCMCRLUpdateHandler(crlStore));
  }

  @Override
  public void subscribe(SubscribeRequest request,
      StreamObserver<SubscribeResponse> responseObserver) {
    UUID clientId;
    try {
      clientId  = clientManager.addClient();
    } catch (SCMException ex) {
      LOG.error("Fail to subscribe for Client.", ex);
      responseObserver.onError(ex);
      return;
    }
    responseObserver.onNext(SubscribeResponse.newBuilder()
        .setClientId(SCMUpdateClientInfo.toClientIdProto(clientId))
        .build());
    responseObserver.onCompleted();
    LOG.info("Client {} subscribed.", clientId);
  }

  @Override
  public void unsubscribe(UnsubscribeRequest request,
      StreamObserver<UnsubscribeResponse> responseObserver) {
    UUID clientId = SCMUpdateClientInfo.fromClientIdProto(
        request.getClientId());
    boolean removed = clientManager.removeClient(clientId);
    if (removed) {
      LOG.info("Client {} unsubscribed.", clientId);
    } else {
      LOG.info("Client {} does not exist, no-op for unsubscribe", clientId);
    }
    responseObserver.onNext(UnsubscribeResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<UpdateRequest> updateStatus(
      StreamObserver<UpdateResponse> responseObserver) {
    return new StreamObserver<UpdateRequest>() {
      @Override
      public void onNext(UpdateRequest updateRequest) {
        LOG.debug("UpdateStatus onNext");
        clientManager.handleClientUpdate(updateRequest, responseObserver);
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.debug("UpdateStatus onError", throwable);
        clientManager.removeClient(responseObserver);
      }

      @Override
      public void onCompleted() {
        LOG.debug("UpdateStatus(Client) onComplete");
        responseObserver.onCompleted();
        clientManager.removeClient(responseObserver);
      }
    };
  }

  // service prepare a update response and broadcast to all clients subscribed.
  public void notifyUpdate(Type type) {
    clientManager.onUpdate(type);
  }
}
