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

package org.apache.hadoop.ozone.om.jobworker.command;

import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;

/**
 * Simple mock implementation of an OMJobworkerCommand for testing.
 */
public class MockOMJobworkerCommand extends OMJobworkerCommand<CommandStatus> {
  private final long id;
  private final OMJobworkerCommandProto.Type type;
  private long expirationTimestampMs;

  public MockOMJobworkerCommand(long id, OMJobworkerCommandProto.Type type) {
    super(id);
    this.id = id;
    this.type = type;
    this.expirationTimestampMs = 0;
  }

  public MockOMJobworkerCommand(long id, OMJobworkerCommandProto.Type type, long expirationTimestampMs) {
    super(id);
    this.id = id;
    this.type = type;
    this.expirationTimestampMs = expirationTimestampMs;
  }

  @Override
  public OMJobworkerCommandProto.Type getType() {
    return type;
  }

  @Override
  public CommandStatus getProto() {
    return CommandStatus.newBuilder()
        .setCmdId(id)
        .setType(type)
        .setStatus(CommandStatus.Status.PENDING)
        .setOmServiceId("test-service")
        .build();
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public long getExpirationTimestampMs() {
    return expirationTimestampMs;
  }
}
