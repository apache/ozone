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

package org.apache.ozone.fs.http.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ozone.lib.service.FileSystemAccessException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HttpFSExceptionProvider}.
 */
public class TestHttpFSExceptionProvider {

  @Test
  public void accessControlExceptionMapsToUnauthorized() {
    HttpFSExceptionProvider provider = new HttpFSExceptionProvider();
    Response response = provider.toResponse(
        new AccessControlException("User not in HttpFSServer admin group"));
    assertThat(response.getStatus())
        .isEqualTo(Response.Status.UNAUTHORIZED.getStatusCode());
  }

  @Test
  public void wrappedAccessControlExceptionMapsToUnauthorized() {
    HttpFSExceptionProvider provider = new HttpFSExceptionProvider();
    Response response = provider.toResponse(new FileSystemAccessException(
        FileSystemAccessException.ERROR.H03,
        new AccessControlException("permission denied")));
    assertThat(response.getStatus())
        .isEqualTo(Response.Status.UNAUTHORIZED.getStatusCode());
  }

  @Test
  public void genericIOExceptionMapsToServerError() {
    HttpFSExceptionProvider provider = new HttpFSExceptionProvider();
    Response response = provider.toResponse(new IOException("io failure"));
    assertThat(response.getStatus())
        .isEqualTo(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
  }
}
