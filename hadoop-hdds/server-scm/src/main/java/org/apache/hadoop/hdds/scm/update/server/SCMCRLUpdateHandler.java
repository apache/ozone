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
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.CRLUpdateResponse;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateResponse;
import org.apache.hadoop.hdds.scm.update.client.CRLStore;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.crl.CRLCodec;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class handle the CRL client update and response.
 */
public class SCMCRLUpdateHandler implements SCMUpdateHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMCRLUpdateHandler.class);
  private final CRLStore crlStore;

  private static final SCMUpdateServiceProtos.Type TYPE =
      SCMUpdateServiceProtos.Type.CRLUpdate;

  private final Map<UUID, CRLClientInfo> clients;

  SCMCRLUpdateHandler(CRLStore crlStore) {
    this.crlStore = crlStore;
    clients = new ConcurrentHashMap<>();
  }

  public SCMUpdateServiceProtos.Type getType() {
    return TYPE;
  }

  @Override
  public void handleClientRequest(SCMUpdateServiceProtos.UpdateRequest request,
      SCMUpdateClientInfo clientInfo) {
    SCMUpdateServiceProtos.CRLUpdateRequest updateStatusRequest =
        request.getCrlUpdateRequest();
    long clientCrlId = updateStatusRequest.getReceivedCrlId();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Client {} updateStatus \nclientCrlId {}  \npendingCrls {}",
          clientInfo.getClientId(), clientCrlId,
          updateStatusRequest.getPendingCrlIdsList().toString());
    }

    CRLClientInfo crlClientInfo;
    if (!clients.containsKey(clientInfo.getClientId())) {
      crlClientInfo = new CRLClientInfo(clientInfo);
      clients.put(clientInfo.getClientId(), crlClientInfo);
    } else {
      crlClientInfo = clients.get(clientInfo.getClientId());
    }

    crlClientInfo.setPendingCrlIds(
        request.getCrlUpdateRequest().getPendingCrlIdsList());
    crlClientInfo.setReceivedCrlId(
        request.getCrlUpdateRequest().getReceivedCrlId());

    sendCrlUpdateToClient(crlClientInfo);
  }

  @Override
  public void onUpdate() {
    LOG.debug("Update due to certificate revocation");
    // server crl id is usually > client crl id when this is invoked.
    clients.values().forEach(client -> {
      sendCrlUpdateToClient(client);
    });
  }

  @Override
  public void onRemoveClient(SCMUpdateClientInfo clientInfo) {
    clients.remove(clientInfo.getClientId());
  }

  private void sendCrlUpdateToClient(CRLClientInfo client) {
    long clientCrlId = client.getReceivedCrlId();
    long serverCrlId = crlStore.getLatestCrlId();

    if (clientCrlId >= serverCrlId) {
      return;
    }

    LOG.debug("## Server: clientCrlId {} serverCrlId {}",
        clientCrlId, serverCrlId);

    long nextCrlId = clientCrlId + 1;
    try {
      CRLInfo crlInfo = null;
      while (crlInfo == null && nextCrlId <= serverCrlId) {
        crlInfo = crlStore.getCRL(nextCrlId);
        nextCrlId++;
      }
      if (crlInfo == null) {
        LOG.debug("Nothing to send to client");
        return;
      }
      sendCrlToClient(crlInfo, client.getUpdateClientInfo());
    } catch (Exception e) {
      LOG.error("Failed to handle client update.", e);
      client.getUpdateClientInfo().getResponseObserver().onError(Status.INTERNAL
          .withDescription("Failed to send crl" + nextCrlId +
              " to client " + client.getUpdateClientInfo().getClientId())
          .asException());
    }
  }

  private void sendCrlToClient(CRLInfo crl, SCMUpdateClientInfo clientInfo)
      throws SCMSecurityException {
    LOG.debug("Sending client# {} with crl: {} ",
        clientInfo.getClientId(), crl.getCrlSequenceID());
    StreamObserver<UpdateResponse> responseObserver =
        clientInfo.getResponseObserver();
    SCMUpdateServiceProtos.CRLInfoProto crlInfoProto =
        SCMUpdateServiceProtos.CRLInfoProto.newBuilder()
            .setCrlSequenceID(crl.getCrlSequenceID())
            .setX509CRL(CRLCodec.getPEMEncodedString(crl.getX509CRL()))
            .setCreationTimestamp(crl.getCreationTimestamp()).build();
    responseObserver.onNext(
        UpdateResponse.newBuilder()
            .setUpdateType(SCMUpdateServiceProtos.Type.CRLUpdate)
            .setCrlUpdateResponse(CRLUpdateResponse.newBuilder()
                .setCrlInfo(crlInfoProto).build()).build());
  }
}
