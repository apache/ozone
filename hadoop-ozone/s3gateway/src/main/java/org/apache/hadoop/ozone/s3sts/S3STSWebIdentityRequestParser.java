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
import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Parses the STS action without logging request parameters.
 */
final class S3STSWebIdentityRequestParser {

  static final String ACTION = "Action";
  static final String ASSUME_ROLE_WITH_WEB_IDENTITY =
      "AssumeRoleWithWebIdentity";

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
      return context.getUriInfo().getQueryParameters().getFirst(ACTION);
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

    byte[] body = readFully(stream);
    context.setEntityStream(new ByteArrayInputStream(body));
    String form = new String(body, StandardCharsets.UTF_8);
    for (String pair : form.split("&")) {
      int equals = pair.indexOf('=');
      if (equals < 0) {
        continue;
      }
      String name = decode(pair.substring(0, equals));
      if (ACTION.equals(name)) {
        return decode(pair.substring(equals + 1));
      }
    }
    return null;
  }

  private static byte[] readFully(InputStream stream) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int read;
    while ((read = stream.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  private static String decode(String value) throws IOException {
    return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
  }
}
