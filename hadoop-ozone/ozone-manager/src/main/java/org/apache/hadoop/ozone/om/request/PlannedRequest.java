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

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.TransitionBuilder;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Abstract base class for the leader-planned execution path.
 * Defines the template: preProcess → authorize → acquireLocks → plan → releaseLocks.
 */
public abstract class PlannedRequest {

  private final OMRequest omRequest;

  protected PlannedRequest(OMRequest omRequest) {
    this.omRequest = omRequest;
  }

  public OMRequest getOmRequest() {
    return omRequest;
  }

  public Type getCmdType() {
    return omRequest.getCmdType();
  }

  public abstract void preProcess(OzoneManager om) throws IOException;

  public abstract void authorize(OzoneManager om) throws IOException;

  public abstract void acquireLocks(OzoneManager om) throws IOException;

  public abstract void releaseLocks(OzoneManager om);

  public abstract void plan(OzoneManager om, TransitionBuilder builder) throws IOException;

  public OMResponse buildErrorResponse(Exception ex) {
    Status status = Status.INTERNAL_ERROR;
    if (ex instanceof OMException) {
      status = Status.valueOf(((OMException) ex).getResult().name());
    }
    return OMResponse.newBuilder()
        .setCmdType(getCmdType())
        .setStatus(status)
        .setSuccess(false)
        .setMessage(ex.getMessage() != null ? ex.getMessage() : ex.toString())
        .build();
  }
}
