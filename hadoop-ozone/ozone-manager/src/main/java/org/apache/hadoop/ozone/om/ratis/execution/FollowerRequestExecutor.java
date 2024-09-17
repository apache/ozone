/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.ratis.execution;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * om executor.
 */
public class FollowerRequestExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(FollowerRequestExecutor.class);
  private static final int RATIS_TASK_POOL_SIZE = 1;
  private static final int RATIS_TASK_QUEUE_SIZE = 1000;
  private final AtomicLong callId = new AtomicLong(0);
  private final OzoneManager ozoneManager;
  private final PoolExecutor<RequestContext> ratisSubmitter;

  public FollowerRequestExecutor(OzoneManager om) {
    this.ozoneManager = om;
    ratisSubmitter = new PoolExecutor<>(RATIS_TASK_POOL_SIZE, RATIS_TASK_QUEUE_SIZE,
        ozoneManager.getThreadNamePrefix(), this::ratisSubmitCommand, null);
  }

  public int batchSize() {
    return RATIS_TASK_POOL_SIZE;
  }

  public void submit(int idx, RequestContext ctx) throws InterruptedException {
    ratisSubmitter.submit(idx, ctx);
  }

  private void ratisSubmitCommand(Collection<RequestContext> ctxs, PoolExecutor<RequestContext> nxtPool) {
    for (RequestContext ctx : ctxs) {
      sendDbUpdateRequest(ctx);
    }
  }

  private void sendDbUpdateRequest(RequestContext ctx) {
    try {
      if (ozoneManager.isRatisEnabled()) {
        throw new IOException("Non-ratis call is not supported");
      }
      OzoneManagerProtocolProtos.OMResponse response = ozoneManager.getOmRatisServer().submitRequest(ctx.getRequest(),
          ClientId.randomId(), callId.incrementAndGet());
      ctx.getFuture().complete(response);
    } catch (IOException ex) {
      ctx.getFuture().complete(createErrorResponse(ctx.getRequest(), ex));
    } catch (Throwable th) {
      ctx.getFuture().complete(createErrorResponse(ctx.getRequest(), new IOException(th)));
    }
  }

  private OzoneManagerProtocolProtos.OMResponse createErrorResponse(
      OzoneManagerProtocolProtos.OMRequest omRequest, IOException exception) {
    OzoneManagerProtocolProtos.OMResponse.Builder omResponseBuilder = OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponseBuilder.setMessage(exception.getMessage());
    }
    OzoneManagerProtocolProtos.OMResponse omResponse = omResponseBuilder.build();
    return omResponse;
  }
}
