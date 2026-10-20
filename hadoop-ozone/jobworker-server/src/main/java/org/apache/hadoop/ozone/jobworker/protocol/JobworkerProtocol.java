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

package org.apache.hadoop.ozone.jobworker.protocol;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;

/**
 * The protocol spoken between Jobworker and OM.
 * For specifics, please the Protoc file {@link JobworkerServiceProtocol.proto} that defines this protocol.
 */
public interface JobworkerProtocol extends Closeable {

  /**
   * Returns OM version.
   *
   * @return Version info.
   */
  GetOMVersionResponse getOMVersion(
      GetOMVersionRequest getOMVersionRequest) throws IOException;

  /**
   * Send a REGISTER request to the OM for Jobworker gRPC server.
   *
   * @param registerJobworkerRequest The registration request.
   * @return The registration response.
   * @throws IOException If gRPC call fails.
   */
  RegisterJobworkerResponse registerJobworker(
      RegisterJobworkerRequest registerJobworkerRequest) throws IOException;

  /**
   * Send a HEARTBEAT request to OM for the Jobworker gRPC server.
   *
   * @param sendHeartbeatRequest The heartbeat request.
   * @return The heartbeat response.
   * @throws IOException If gRPC call fails.
   */
  SendHeartbeatResponseProto sendHeartbeat(
      SendHeartbeatRequest sendHeartbeatRequest) throws IOException;
}
