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

package org.apache.hadoop.ozone.recon;

import com.google.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Recon API Response Utility class.
 */
@Singleton
public final class ReconResponseUtils {

  // Declared a private constructor to avoid checkstyle issues.
  private ReconResponseUtils() {

  }

  /**
   * Returns a response indicating that no keys matched the search prefix.
   *
   * @param startPrefix The search prefix that was used.
   * @return The response indicating that no keys matched the search prefix.
   */
  public static Response noMatchedKeysResponse(String startPrefix) {
    String jsonResponse = String.format(
        "{\"message\": \"No keys matched the search prefix: '%s'.\"}",
        startPrefix);
    return Response.status(Response.Status.NO_CONTENT)
        .entity(jsonResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Utility method to create a bad request response with a custom message.
   * Which means the request sent by the client to the server is incorrect
   * or malformed and cannot be processed by the server.
   *
   * @param message The message to include in the response body.
   * @return A Response object configured with the provided message.
   */
  public static Response createBadRequestResponse(String message) {
    String jsonResponse = String.format("{\"message\": \"%s\"}", message);
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(jsonResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Utility method to create an internal server error response with a custom message.
   * Which means the server encountered an unexpected condition that prevented it
   * from fulfilling the request.
   *
   * @param message The message to include in the response body.
   * @return A Response object configured with the provided message.
   */
  public static Response createInternalServerErrorResponse(String message) {
    String jsonResponse = String.format("{\"message\": \"%s\"}", message);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(jsonResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
