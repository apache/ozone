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

package org.apache.hadoop.ozone.s3.signature;

import javax.ws.rs.container.ContainerRequestContext;
import org.apache.hadoop.ozone.audit.AuditAction;

/**
 * Authentication/authorization actions for S3Gateway.
 */
public class AuthOperation implements AuditAction {
  private final String path;
  private final String method;

  public AuthOperation(final String path, final String method) {
    this.path = path;
    this.method = method;
  }

  public static AuthOperation fromContext(ContainerRequestContext context) {
    return new AuthOperation("/" + context.getUriInfo().getPath(),
        context.getMethod());
  }

  @Override
  public String getAction() {
    return String.format("AUTH(%s %s)", method, path);
  }
}
