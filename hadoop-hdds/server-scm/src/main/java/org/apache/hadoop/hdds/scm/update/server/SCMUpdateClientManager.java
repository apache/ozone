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

import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.Type;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that manages SCM update clients.
 */
public class SCMUpdateClientManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMUpdateClientManager.class);
  private Map<UUID, SCMUpdateClientInfo> clients;
  private Map<Type, SCMUpdateHandler> handlers;

  public SCMUpdateClientManager() {
    clients = new ConcurrentHashMap<>();
    handlers = new ConcurrentHashMap<>();
  }

  public void registerHandler(SCMUpdateHandler handler) {
    handlers.put(handler.getType(), handler);
  }

  public void unRegisterHandler(Type type) {
    handlers.remove(type);
  }

  public UUID addClient() throws SCMException {
    UUID clientId = UUID.randomUUID();
    int retryCount = 5;
    while (clients.containsKey(clientId)) {
      if (retryCount > 0) {
        clientId = UUID.randomUUID();
        retryCount--;
      } else {
        throw new SCMException("Failed to add CRL client with random clientId" +
            " collision", SCMException.ResultCodes.FAILED_TO_ADD_CRL_CLIENT);
      }
    }

    SCMUpdateClientInfo clientInfo = new SCMUpdateClientInfo(clientId);
    clients.put(clientId, clientInfo);
    return clientId;
  }

  // this does not necessarily produce a server response via responseObserver.
  public void handleClientUpdate(UpdateRequest request,
      StreamObserver<UpdateResponse> responseObserver) {
    UUID clientId = SCMUpdateClientInfo.fromClientIdProto(
        request.getClientId());

    // Unknown client update
    if (!clients.containsKey(clientId)) {
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription("Client must subscribe before it can " +
              "send/receive updates")
          .asException());
    }

    // record the server to client channel
    SCMUpdateClientInfo clientInfo = clients.get(clientId);
    if (clientInfo.getResponseObserver() == null) {
      clientInfo.setResponseObserver(responseObserver);
    }

    if (handlers.containsKey(request.getUpdateType())) {
      handlers.get(request.getUpdateType())
          .handleClientRequest(request, clientInfo);
    } else {
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription("Unknown client update type.")
          .asException());
    }
  }

  /**
   * Remove client by client Id.
   * @param clientId - client Id
   * @return true if client is removed, false otherwise.
   */
  public boolean removeClient(UUID clientId) {
    if (clients.containsKey(clientId)) {
      SCMUpdateClientInfo clientInfo = clients.remove(clientId);
      handlers.values().forEach(handler -> handler.onRemoveClient(clientInfo));
      LOG.info("Client {} removed.", clientId);
      return true;
    }
    return false;
  }

  /**
   * Remove client by its responseObserver obj.
   * @param responseObserver - response observer of the client
   * @return true if client is removed, false otherwise.
   */
  public boolean removeClient(StreamObserver<UpdateResponse> responseObserver) {
    UUID clientId = null;
    for (SCMUpdateClientInfo client : clients.values()) {
      if (client.getResponseObserver() == responseObserver) {
        clientId = client.getClientId();
        break;
      }
    }
    if (clientId != null) {
      LOG.debug("Remove client {} by responseObserver", clientId);
      removeClient(clientId);
      return true;
    }
    LOG.debug("Remove client {} by responseObserver not found!");
    return false;
  }

  public void onUpdate(Type type) {
    if (handlers.containsKey(type)) {
      handlers.get(type).onUpdate();
    } else {
      LOG.warn("Unknown update type to broadcast!");
    }
  }
}

