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

package org.apache.hadoop.ozone.jobworker.commands;

import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.ozone.test.GenericTestUtils;

/**
 * Helpers for waiting for asynchronous command execution and creating test statuses.
 */
public final class CommandTestUtils {
  private CommandTestUtils() {
  }

  public static void waitTillFinishExecution(AbstractJobworkerCommandHandler closeHandler)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> closeHandler.getQueuedCount() <= 0, 10, 3000);
  }

  public static JobworkerCommandStatus createJobworkerCommandStatus(JobworkerCommand command) {
    return JobworkerCommandStatus.newBuilder()
        .setOmServiceId(command.getOmServiceId())
        .setStatus(JobworkerServiceProtocolProtos.CommandStatus.Status.PENDING)
        .setType(JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type.mockCommand)
        .setCmdId(command.getId())
        .build();
  }
}
