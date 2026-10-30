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

package org.apache.hadoop.ozone.jobworker.client;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.jobworker.protocolPB.JobworkerProtocolClientSideTranslatorPB;

/**
 * Jobworker User side Client.
 */
public class JobworkerClient implements Closeable {
  private final JobworkerProtocol translator;

  /**
   * Creates a gRPC client to communicate with the JobworkerService.
   *
   * @param omAddress The OM host Address.
   * @param conf      The OzoneConfiguration
   */
  public JobworkerClient(String omAddress, OzoneConfiguration conf) {
    this.translator = createOzoneManagerClient(
        new JobworkerProtocolClientSideTranslatorPB(omAddress, conf), conf);
  }

  protected JobworkerProtocol createOzoneManagerClient(
      JobworkerProtocolClientSideTranslatorPB jobworkerProtocolClientSideTranslatorPB,
      OzoneConfiguration conf) {
    return TracingUtil.createProxy(jobworkerProtocolClientSideTranslatorPB,
        JobworkerProtocol.class, conf);
  }

  /**
   * Returns OM version.
   *
   * @throws IOException If the request fails.
   */
  public GetOMVersionResponse getOMVersion() throws IOException {
    GetOMVersionRequest request = GetOMVersionRequest.newBuilder().build();
    return translator.getOMVersion(request);
  }

  /**
   * Send a REGISTER request to the OM for Jobworker gRPC server.
   *
   * @param registerRequest The registration request.
   * @return The registration response.
   * @throws IOException If the request fails.
   */
  public RegisterJobworkerResponse register(
      RegisterJobworkerRequest registerRequest) throws IOException {
    return translator.registerJobworker(registerRequest);
  }

  /**
   * Send a HEARTBEAT request to OM for the Jobworker gRPC server.
   *
   * @param heartbeatRequest The heartbeat request.
   * @return The heartbeat response.
   * @throws IOException If the request fails.
   */
  public SendHeartbeatResponseProto sendHeartbeat(
      SendHeartbeatRequest heartbeatRequest) throws IOException {
    return translator.sendHeartbeat(heartbeatRequest);
  }

  @Override
  public void close() throws IOException {
    translator.close();
  }
}
