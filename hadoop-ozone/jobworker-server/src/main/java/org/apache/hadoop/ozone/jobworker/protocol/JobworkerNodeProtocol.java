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

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;

/**
 * The protocol to maintain jobworker status on the OM side.
 */
@InterfaceAudience.Private
public interface JobworkerNodeProtocol {

  /**
   * Gets the version info from OM.
   *
   * @param versionRequest - version Request.
   * @return - returns OM version info and other required information needed
   * by jobworker.
   */
  GetOMVersionResponse getVersion(GetOMVersionRequest versionRequest);

  /**
   * Send a REGISTER request to the OM for Jobworker gRPC server.
   *
   * @throws IOException If gRPC call fails.
   */
  RegisterJobworkerResponse registerJobworker(JobworkerDetails jobworkerDetails) throws IOException;

  /**
   * Check if node is registered or not.
   * Return true if Node is registered and false otherwise.
   *
   * @param jobworkerID - Jobworker UUID.
   * @return true if Node is registered, false otherwise
   */
  Boolean isJobworkerNodeRegistered(UUID jobworkerID);

  /**
   * Process Jobworker heartbeat.
   * @param jobworkerDetails - jobworkerDetails.
   */
  void processHeartbeat(JobworkerDetails jobworkerDetails);

  /**
   * Returns a list of Commands for the jobworker.
   * @param jobworkerId jobworker UUID
   * @return List of OMJobworkerCommand Commands.
   */
  List<OMJobworkerCommand> pollJobworkerCommand(UUID jobworkerId);
}
