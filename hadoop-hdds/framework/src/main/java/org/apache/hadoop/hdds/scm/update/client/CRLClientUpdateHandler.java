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

package org.apache.hadoop.hdds.scm.update.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceGrpc.SCMUpdateServiceStub;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.CRLUpdateRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateResponse;
import org.apache.hadoop.hdds.scm.update.server.SCMUpdateClientInfo;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * CRL client update handler that handles local CRL update and pending CRLs.
 */
public class CRLClientUpdateHandler implements ClientUpdateHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      CRLClientUpdateHandler.class);
  private static final String NAME = "CRLClientUpdateHandler";

  private final SCMUpdateServiceStub updateStub;
  private final ClientCRLStore clientStore;

  // Used to update server about local pending crl id list
  private StreamObserver<UpdateRequest> requestObserver;
  private UUID clientUuid;
  private SCMUpdateServiceProtos.ClientId clientIdProto;

  // periodically process pending crls
  private ScheduledExecutorService executorService;
  private final SCMUpdateServiceGrpcClient serviceGrpcClient;
  private long crlCheckInterval;

  CRLClientUpdateHandler(UUID clientId,
      SCMUpdateServiceStub updateStub,
      SCMUpdateServiceGrpcClient serviceGrpcClient,
      long crlCheckInterval) {
    this.clientUuid = clientId;
    this.updateStub = updateStub;
    this.serviceGrpcClient = serviceGrpcClient;

    this.clientStore = serviceGrpcClient.getClientCRLStore();
    this.crlCheckInterval = crlCheckInterval;
    LOG.info("Pending CRL check interval : {}s", crlCheckInterval/1000);
    this.executorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("CRLUpdateHandler Thread - %d").build());
  }

  public static Logger getLog() {
    return LOG;
  }

  @Override
  public void handleServerUpdate(UpdateResponse updateResponse) {
    SCMUpdateServiceProtos.CRLInfoProto crlInfo =
        updateResponse.getCrlUpdateResponse().getCrlInfo();

    long receivedCrlId = crlInfo.getCrlSequenceID();
    long localCrlId = clientStore.getLatestCrlId();

    LOG.debug("## Client: clientId {} clientCrlId {} receivedCrlId {}",
        clientUuid, localCrlId, receivedCrlId);
    if (localCrlId == receivedCrlId) {
      return;
    }
    // send a client update to refresh stale server
    if (localCrlId > receivedCrlId) {
      LOG.warn("Received stale crlId {} lower than client crlId {}",
          receivedCrlId, localCrlId);
      sendClientUpdate();
      return;
    }

    CRLInfo crl;
    try {
      crl = CRLInfo.fromCRLProto3(crlInfo);
    } catch (Exception e) {
      LOG.error("Can't parse server CRL update, skip...", e);
      return;
    }
    clientStore.onRevokeCerts(crl);
    // send client update.
    sendClientUpdate();
  }

  public void start() {
    // send initial update request to get a request observer handle
    UpdateRequest updateReq = getUpdateRequest();
    requestObserver = updateStub.withWaitForReady()
        .updateStatus(new StreamObserver<UpdateResponse>() {
          @Override
          public void onNext(UpdateResponse updateResponse) {
            LOG.debug("Receive server response: {}", updateResponse.toString());
            serviceGrpcClient.incrUpdateCount();
            handleServerUpdate(updateResponse);
          }

          @Override
          public void onError(Throwable throwable) {
            LOG.debug("Receive server error ", throwable);
            serviceGrpcClient.incrErrorCount();
            if (serviceGrpcClient.getIsRunning().get()) {
              // TODO: not all server error needs client restart.
              LOG.warn("Restart client on server error: ", throwable);
              serviceGrpcClient.restart();
            }
          }

          @Override
          public void onCompleted() {
            LOG.debug("Receive server completed");
          }
        });
    requestObserver.onNext(updateReq);
    startPendingCrlChecker();
  }

  public void stop() {
    stopPendingCrlCheck();
  }

  private void stopPendingCrlCheck() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Unexpected exception while waiting for executor service" +
          " to shutdown", e);
    }
  }

  private void startPendingCrlChecker() {
    executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        // background thread handle pending crl and update server

        CRLInfo crl = null;
        while ((crl = clientStore.getNextPendingCrl()) != null) {
          if (crl.shouldRevokeNow()) {
            serviceGrpcClient.incrPendingCrlRemoveCount();
            LOG.info("Time to process crlId {}", crl.getCrlSequenceID());
            clientStore.removePendingCrl(crl);
            sendClientUpdate();
          } else {
            // we are done with this pending Crl, wait for next round
            break;
          }
        }
      }
    }, 0, crlCheckInterval, TimeUnit.MILLISECONDS);
  }

  private void sendClientUpdate() {
    requestObserver.onNext(getUpdateRequest());
  }

  private UpdateRequest getUpdateRequest() {
    return UpdateRequest.newBuilder()
        .setUpdateType(SCMUpdateServiceProtos.Type.CRLUpdate)
        .setClientId(SCMUpdateClientInfo.toClientIdProto(clientUuid))
        .setCrlUpdateRequest(getCrlUpdateRequest())
        .build();
  }

  private CRLUpdateRequest getCrlUpdateRequest() {
    List<Long> pendingCrlIds = clientStore.getPendingCrlIds();
    return CRLUpdateRequest.newBuilder()
        .setReceivedCrlId(clientStore.getLatestCrlId())
        .addAllPendingCrlIds(pendingCrlIds)
        .build();
  }
}
