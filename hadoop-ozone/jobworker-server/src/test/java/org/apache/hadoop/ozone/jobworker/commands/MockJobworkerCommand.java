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

import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerMockCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;

/**
 * Simple mock implementation of a JobworkerCommand.
 */
public class MockJobworkerCommand extends JobworkerCommand<JobworkerMockCommandProto> {
  private final OMJobworkerCommandProto.Type type;
  private final JobworkerMockCommandProto proto;
  private static final String OM_SERVICE_ID = "omServiceId";

  public MockJobworkerCommand(long id, OMJobworkerCommandProto.Type type) {
    this(id, OM_SERVICE_ID, type);
  }

  public MockJobworkerCommand(long id, String omServiceId, OMJobworkerCommandProto.Type type) {
    this(id, omServiceId, 0, 0, type, JobworkerMockCommandProto.getDefaultInstance());
  }

  public MockJobworkerCommand(long id, String omServiceId, long term, long expirationTimestampMs,
                              OMJobworkerCommandProto.Type type) {
    this(id, omServiceId, term, expirationTimestampMs, type, JobworkerMockCommandProto.getDefaultInstance());

  }

  public MockJobworkerCommand(long id, String omServiceId, long term, long expirationTimestampMs,
                              OMJobworkerCommandProto.Type type, JobworkerMockCommandProto proto) {
    super(id, omServiceId, term, expirationTimestampMs);
    this.type = type;
    this.proto = proto;
  }

  @Override
  public OMJobworkerCommandProto.Type getType() {
    return type;
  }

  @Override
  public JobworkerMockCommandProto getProto() {
    return proto;
  }
}
