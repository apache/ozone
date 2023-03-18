/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.throttler;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util methods for throttler filter and scheduler.
 */
public final class ThrottlerUtils {
  public static final Request EMPTY_REQUEST;

  static {
    EMPTY_REQUEST = new Request() {
      @Override
      public Map<String, String> getHeaders() {
        return null;
      }

      @Override
      public Map<String, String> getQueryParameters() {
        return null;
      }
    };
  }

  private ThrottlerUtils() {

  }

  /**
   *  Converts a MultivaluedMap to a map by taking the first value for each
   *  corresponding key.
   * <p>
   * This method must be used with MultivaluedMap where each key has a list
   * of a single value.
   *
   * @param map MultivaluedMap where each value is a single entry list
   * @return a map where each key has a single value
   */
  public static Map<String, String> toMap(MultivaluedMap<String, String> map) {
    Map<String, String> res = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      res.put(entry.getKey(), entry.getValue().get(0));
    }
    return res;
  }

  /**
   * Returns a new Request instance out of ContainerRequestContext if decay
   * scheduler feature is enabled, otherwise a request with no headers nor
   * query params.
   *
   * @param ctx container request context
   * @param ozoneConfiguration ozone configurations
   * @return request out of ctx headers and query param
   */
  public static Request toRequest(ContainerRequestContext ctx,
                                  OzoneConfiguration ozoneConfiguration) {
    if (!ozoneConfiguration.getBoolean(
        S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_ENABLE_KEY,
        S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_ENABLE_DEFAULT)) {
      return EMPTY_REQUEST;
    }

    return new Request() {
      @Override
      public Map<String, String> getHeaders() {
        return toMap(ctx.getHeaders());
      }

      @Override
      public Map<String, String> getQueryParameters() {
        return toMap(ctx.getUriInfo().getQueryParameters());
      }
    };
  }
}
