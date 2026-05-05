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

package org.apache.hadoop.ozone.container.common.transport.server;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC ServerTransportFilter that limits the number of concurrent
 * connections to the Datanode gRPC server.
 *
 * <p>When the connection limit is reached, new connections are rejected
 * with a RESOURCE_EXHAUSTED status, preventing file descriptor exhaustion.
 */
public class GrpcConnectionLimitFilter extends ServerTransportFilter {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcConnectionLimitFilter.class);

  private final int maxConnections;
  private final AtomicInteger activeConnections = new AtomicInteger(0);
  private final AtomicLong totalAcceptedConnections = new AtomicLong(0);
  private final AtomicLong totalRejectedConnections = new AtomicLong(0);

  /**
   * Creates a new connection limit filter.
   *
   * @param maxConnections the maximum number of concurrent connections allowed.
   *                       If <= 0, no limit is enforced.
   */
  public GrpcConnectionLimitFilter(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  @Override
  public Attributes transportReady(Attributes transportAttrs) {
    if (maxConnections <= 0) {
      return super.transportReady(transportAttrs);
    }

    int current = activeConnections.incrementAndGet();
    if (current > maxConnections) {
      activeConnections.decrementAndGet();
      totalRejectedConnections.incrementAndGet();
      LOG.warn("Connection rejected: limit {} reached, current active: {}",
          maxConnections, current - 1);
      throw new StatusRuntimeException(
          Status.RESOURCE_EXHAUSTED.withDescription(
              "Datanode connection limit exceeded: " + maxConnections));
    }

    totalAcceptedConnections.incrementAndGet();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connection accepted: active connections: {}/{}",
          current, maxConnections);
    }
    return super.transportReady(transportAttrs);
  }

  @Override
  public void transportTerminated(Attributes transportAttrs) {
    if (maxConnections > 0) {
      int current = activeConnections.decrementAndGet();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connection terminated: active connections: {}/{}",
            current, maxConnections);
      }
    }
    super.transportTerminated(transportAttrs);
  }

  public int getActiveConnections() {
    return activeConnections.get();
  }

  public long getTotalAcceptedConnections() {
    return totalAcceptedConnections.get();
  }

  public long getTotalRejectedConnections() {
    return totalRejectedConnections.get();
  }

  public int getMaxConnections() {
    return maxConnections;
  }
}
