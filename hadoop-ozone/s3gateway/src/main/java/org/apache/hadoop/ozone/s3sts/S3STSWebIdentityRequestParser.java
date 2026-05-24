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

package org.apache.hadoop.ozone.s3sts;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

/**
 * Parses the STS action without logging request parameters.
 */
final class S3STSWebIdentityRequestParser {

  static final String ACTION = "Action";
  static final String ASSUME_ROLE_WITH_WEB_IDENTITY =
      "AssumeRoleWithWebIdentity";
  static final int MAX_FORM_BODY_BYTES = 64 * 1024;

  private static final int BUFFER_SIZE = 8192;
  private static final int HTTP_PAYLOAD_TOO_LARGE = 413;

  private S3STSWebIdentityRequestParser() {
  }

  static boolean isAssumeRoleWithWebIdentity(ContainerRequestContext context)
      throws IOException {
    String action = getAction(context);
    return ASSUME_ROLE_WITH_WEB_IDENTITY.equals(action);
  }

  private static String getAction(ContainerRequestContext context)
      throws IOException {
    String method = context.getMethod();
    if (HttpMethod.GET.equalsIgnoreCase(method)) {
      return singleAction(context.getUriInfo().getQueryParameters()
          .get(ACTION));
    }
    if (HttpMethod.POST.equalsIgnoreCase(method)) {
      return getFormAction(context);
    }
    return null;
  }

  private static String getFormAction(ContainerRequestContext context)
      throws IOException {
    InputStream stream = context.getEntityStream();
    if (stream == null) {
      return null;
    }

    byte[] body = readBoundedFormBody(stream);
    context.setEntityStream(new ByteArrayInputStream(body));
    String form = new String(body, StandardCharsets.UTF_8);
    String action = null;
    for (String pair : form.split("&")) {
      int equals = pair.indexOf('=');
      if (equals < 0) {
        continue;
      }
      String name = decode(pair.substring(0, equals));
      if (ACTION.equals(name)) {
        if (action != null) {
          return null;
        }
        action = decode(pair.substring(equals + 1));
      }
    }
    return action;
  }

  private static String decode(String value) throws IOException {
    return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
  }

  private static byte[] readBoundedFormBody(InputStream stream)
      throws IOException {
    ByteArrayOutputStream body =
        new ByteArrayOutputStream(Math.min(MAX_FORM_BODY_BYTES, BUFFER_SIZE));
    byte[] buffer = new byte[BUFFER_SIZE];
    int remaining = MAX_FORM_BODY_BYTES;
    while (remaining > 0) {
      int read = stream.read(buffer, 0, Math.min(buffer.length, remaining));
      if (read == -1) {
        return body.toByteArray();
      }
      body.write(buffer, 0, read);
      remaining -= read;
    }
    if (stream.read() != -1) {
      throw new WebApplicationException(
          "STS WebIdentity request body is too large",
          Response.status(HTTP_PAYLOAD_TOO_LARGE).build());
    }
    return body.toByteArray();
  }

  private static String singleAction(List<String> values) {
    return values != null && values.size() == 1 ? values.get(0) : null;
  }
}
