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

package org.apache.hadoop.ozone.s3.endpoint;

import jakarta.annotation.Nullable;
import java.io.IOException;
import org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.util.Time;

class S3RequestContext {
  private final long startNanos;
  private final PerformanceStringBuilder perf;
  private final EndpointBase endpoint;
  private S3GAction action;
  private OzoneVolume volume;

  S3RequestContext(EndpointBase endpoint, S3GAction action) {
    this.endpoint = endpoint;
    this.startNanos = Time.monotonicNowNanos();
    this.perf = new PerformanceStringBuilder();
    this.action = action;
  }

  long getStartNanos() {
    return startNanos;
  }

  PerformanceStringBuilder getPerf() {
    return perf;
  }

  OzoneVolume getVolume() throws IOException {
    if (volume == null) {
      volume = endpoint.getVolume();
    }
    return volume;
  }

  S3GAction getAction() {
    return action;
  }

  void setAction(S3GAction action) {
    this.action = action;
  }

  /**
   * This method should be called by each handler with the {@code S3GAction} decided based on request parameters,
   * {@code null} if it does not handle the request.  {@code action} is stored, if not null, for use in audit logging.
   *
   * @param a action as determined by handler
   * @return true if handler should ignore the request (i.e. if {@code null} is passed)
   */
  boolean ignore(@Nullable S3GAction a) {
    final boolean ignore = a == null;
    if (!ignore) {
      setAction(a);
    }
    return ignore;
  }
}
