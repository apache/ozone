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

package org.apache.hadoop.ozone.s3.util;

import static org.apache.hadoop.ozone.s3.ClientIpFilter.CLIENT_IP_HEADER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Common utilities for operation auditing purposes.
 */
public final class AuditUtils {
  private AuditUtils() {
  }

  public static Map<String, String> getAuditParameters(
      ContainerRequestContext context) {
    Map<String, String> res = new HashMap<>();
    if (context != null) {
      for (Map.Entry<String, List<String>> entry :
          context.getUriInfo().getPathParameters().entrySet()) {
        res.put(entry.getKey(), entry.getValue().toString());
      }
      for (Map.Entry<String, List<String>> entry :
          context.getUriInfo().getQueryParameters().entrySet()) {
        res.put(entry.getKey(), entry.getValue().toString());
      }
    }
    return res;
  }

  public static String getClientIpAddress(ContainerRequestContext context) {
    return context.getHeaderString(CLIENT_IP_HEADER);
  }
}
