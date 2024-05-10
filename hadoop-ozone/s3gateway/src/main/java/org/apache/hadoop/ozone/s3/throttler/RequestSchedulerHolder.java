/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.throttler;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;

import javax.enterprise.inject.Produces;

/**
 * Request scheduler factory.
 */
public final class RequestSchedulerHolder {
  private static RequestScheduler requestScheduler;

  public static void buildRequestScheduler(OzoneConfiguration configuration) {
    RequestSchedulerHolder.requestScheduler = null;
    if (configuration.getBoolean(
          S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_ENABLE_KEY,
          S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_ENABLE_DEFAULT)) {
      requestScheduler =
          new DecayRequestScheduler(new UserIdentityProvider(), configuration);
    } else {
      requestScheduler = new DefaultRequestScheduler();
    }
  }

  @Produces
  public RequestScheduler scheduler() {
    return requestScheduler;
  }
}
