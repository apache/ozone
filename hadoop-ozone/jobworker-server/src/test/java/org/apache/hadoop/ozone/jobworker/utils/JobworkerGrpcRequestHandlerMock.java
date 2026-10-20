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

package org.apache.hadoop.ozone.jobworker.utils;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceGrpc;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Mock class for handling gRPC requests from JobworkerService for testing.
 */
public class JobworkerGrpcRequestHandlerMock extends
    JobworkerServiceGrpc.JobworkerServiceImplBase {
  private static final String OM_SERVICE_ID1 = "omServiceId1";
  private static final String CLUSTER_ID1 = "test-cluster1";
  private final AtomicInteger versionCallCount = new AtomicInteger(0);
  private final AtomicInteger registerCallCount = new AtomicInteger(0);
  private final AtomicInteger heartbeatCallCount = new AtomicInteger(0);
  private volatile long versionDelayMs = 0;
  private volatile long registerDelayMs = 0;
  private volatile long heartbeatDelayMs = 0;

  @Override
  public void submitRequest(JobworkerServiceProtocolProtos.JobworkerRequest request,
                            StreamObserver<JobworkerServiceProtocolProtos.JobworkerResponse> responseObserver) {
    JobworkerServiceProtocolProtos.JobworkerResponse.Builder responseBuilder =
        JobworkerServiceProtocolProtos.JobworkerResponse.newBuilder()
            .setStatus(JobworkerServiceProtocolProtos.JobworkerResponse.Status.OK);

    try {
      switch (request.getCmdType()) {
      case GET_OM_VERSION:
        if (versionDelayMs > 0) {
          Thread.sleep(versionDelayMs);
        }
        versionCallCount.incrementAndGet();
        responseBuilder.setCmdType(JobworkerServiceProtocolProtos.JobworkerCommandType.GET_OM_VERSION);
        JobworkerServiceProtocolProtos.GetOMVersionResponse
            versionResponse = JobworkerServiceProtocolProtos.GetOMVersionResponse.newBuilder()
            .setSoftwareVersion(1)
            .setDurableJobProtocolVersion(1)
            .addKeys(HddsProtos.KeyValue.newBuilder()
                .setKey(OzoneConsts.OM_ID)
                .setValue("om1")
                .build())
            .addKeys(HddsProtos.KeyValue.newBuilder()
                .setKey(OzoneConsts.CLUSTER_ID)
                .setValue(CLUSTER_ID1)
                .build())
            .addKeys(HddsProtos.KeyValue.newBuilder()
                .setKey(OzoneConsts.OM_SERVICE_ID)
                .setValue(OM_SERVICE_ID1)
                .build())
            .build();
        responseBuilder.setGetOMVersionResponse(versionResponse);
        break;
      case REGISTER_JOBWORKER:
        if (registerDelayMs > 0) {
          Thread.sleep(registerDelayMs);
        }
        registerCallCount.incrementAndGet();
        responseBuilder.setCmdType(JobworkerServiceProtocolProtos.JobworkerCommandType.REGISTER_JOBWORKER);
        JobworkerServiceProtocolProtos.RegisterJobworkerResponse
            registerResponse = JobworkerServiceProtocolProtos.RegisterJobworkerResponse.newBuilder()
            .setJobworkerUUID(request.getRegisterJobworkerRequest()
                .getExtendedJobWorkDetailsProto()
                .getJobworkerDetails()
                .getUuid128())
            .setClusterID(CLUSTER_ID1)
            .setOmServiceId(OM_SERVICE_ID1)
            .setHostname("localhost")
            .setIpAddress("127.0.0.1")
            .setReturnCode(JobworkerServiceProtocolProtos.RegisterJobworkerResponse.ReturnCode.SUCCESS)
            .build();
        responseBuilder.setRegisterJobworkerResponse(registerResponse);
        break;
      case SEND_HEARTBEAT:
        if (heartbeatDelayMs > 0) {
          Thread.sleep(heartbeatDelayMs);
        }
        heartbeatCallCount.incrementAndGet();
        responseBuilder.setCmdType(JobworkerServiceProtocolProtos.JobworkerCommandType.SEND_HEARTBEAT);
        JobworkerServiceProtocolProtos.SendHeartbeatResponseProto
            heartbeatResponse = JobworkerServiceProtocolProtos.SendHeartbeatResponseProto.newBuilder()
            .setJobworkerUUID(request.getSendHeartbeatRequest().getJobworkerDetails().getUuid128())
            .setOmServiceId(OM_SERVICE_ID1)
            .setTerm(1)
            .build();
        responseBuilder.setSendHeartbeatResponseProto(heartbeatResponse);
        break;
      default:
        responseObserver.onError(new IllegalArgumentException("Unknown command type"));
        return;
      }

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      responseObserver.onError(e);
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  public int getVersionCallCount() {
    return versionCallCount.get();
  }

  public int getRegisterCallCount() {
    return registerCallCount.get();
  }

  public int getHeartbeatCallCount() {
    return heartbeatCallCount.get();
  }

  public void setGetVersionDelayMs(long delay) {
    this.versionDelayMs = delay;
  }

  public void setRegisterDelayMs(long delay) {
    this.registerDelayMs = delay;
  }

  public void setHeartbeatDelayMs(long delay) {
    this.heartbeatDelayMs = delay;
  }

  public void resetCallCount() {
    versionCallCount.set(0);
    registerCallCount.set(0);
    heartbeatCallCount.set(0);
  }

  public void resetDelayMs() {
    versionDelayMs = 0;
    registerDelayMs = 0;
    heartbeatDelayMs = 0;
  }
}
