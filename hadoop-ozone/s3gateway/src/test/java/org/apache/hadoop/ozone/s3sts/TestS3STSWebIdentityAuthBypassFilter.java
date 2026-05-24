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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.AuthorizationFilter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for the STS-only WebIdentity auth bypass filter.
 */
public class TestS3STSWebIdentityAuthBypassFilter {

  @Test
  public void enabledGetWebIdentityRequestSkipsAwsAuth() throws Exception {
    S3STSWebIdentityAuthBypassFilter filter = filter(true);
    ContainerRequestContext context = context(HttpMethod.GET,
        "AssumeRoleWithWebIdentity", null);

    filter.filter(context);

    verify(context).setProperty(AuthorizationFilter.SKIP_AWS_AUTH_PROPERTY,
        Boolean.TRUE);
  }

  @Test
  public void disabledWebIdentityRequestDoesNotSkipAwsAuth()
      throws Exception {
    S3STSWebIdentityAuthBypassFilter filter = filter(false);
    ContainerRequestContext context = context(HttpMethod.GET,
        "AssumeRoleWithWebIdentity", null);

    filter.filter(context);

    verify(context, never()).setProperty(anyString(), any());
  }

  @Test
  public void otherStsActionDoesNotSkipAwsAuth() throws Exception {
    S3STSWebIdentityAuthBypassFilter filter = filter(true);
    ContainerRequestContext context = context(HttpMethod.GET, "AssumeRole",
        null);

    filter.filter(context);

    verify(context, never()).setProperty(anyString(), any());
  }

  @Test
  public void postWebIdentityRequestSkipsAwsAuthAndRestoresBody()
      throws Exception {
    S3STSWebIdentityAuthBypassFilter filter = filter(true);
    String body = "Action=AssumeRoleWithWebIdentity"
        + "&WebIdentityToken=sensitive-token-material";
    ContainerRequestContext context = context(HttpMethod.POST, null, body);

    filter.filter(context);

    verify(context).setProperty(AuthorizationFilter.SKIP_AWS_AUTH_PROPERTY,
        Boolean.TRUE);
    ArgumentCaptor<InputStream> streamCaptor =
        ArgumentCaptor.forClass(InputStream.class);
    verify(context).setEntityStream(streamCaptor.capture());
    assertEquals(body, read(streamCaptor.getValue()));
  }

  @Test
  public void adversarialActionValuesDoNotSkipAwsAuth() throws Exception {
    assertDoesNotSkipPost("Action=AssumeRoleWithWebIdentity%20");
    assertDoesNotSkipPost("action=assumerolewithwebidentity");
    assertDoesNotSkipPost("Action=AssumeRoleWithWebIdentity%00");
    assertDoesNotSkipPost("Action=AssumeRoleWithWebIdentity"
        + "&Action=AssumeRole");
    assertDoesNotSkipPost("{\"Action\":\"AssumeRoleWithWebIdentity\"}");
    assertDoesNotSkipPost("Version=2011-06-15");
    assertDoesNotSkipPost("Action=");
  }

  @Test
  public void duplicateQueryActionDoesNotSkipAwsAuth() throws Exception {
    S3STSWebIdentityAuthBypassFilter filter = filter(true);
    ContainerRequestContext context = contextWithQueryActions(
        "AssumeRoleWithWebIdentity", "AssumeRole");

    filter.filter(context);

    verify(context, never()).setProperty(anyString(), any());
  }

  private static S3STSWebIdentityAuthBypassFilter filter(boolean enabled) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_STS_WEB_IDENTITY_ENABLED, enabled);
    return new S3STSWebIdentityAuthBypassFilter(conf);
  }

  private static void assertDoesNotSkipPost(String body) throws Exception {
    S3STSWebIdentityAuthBypassFilter filter = filter(true);
    ContainerRequestContext context = context(HttpMethod.POST, null, body);

    filter.filter(context);

    verify(context, never()).setProperty(anyString(), any());
  }

  private static ContainerRequestContext context(String method,
      String queryAction, String body) {
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    MultivaluedHashMap<String, String> queryParams =
        new MultivaluedHashMap<>();
    if (queryAction != null) {
      queryParams.putSingle("Action", queryAction);
    }
    when(context.getMethod()).thenReturn(method);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getQueryParameters()).thenReturn(queryParams);
    if (body != null) {
      when(context.getEntityStream()).thenReturn(new ByteArrayInputStream(
          body.getBytes(StandardCharsets.UTF_8)));
    }
    return context;
  }

  private static ContainerRequestContext contextWithQueryActions(
      String... queryActions) {
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    MultivaluedHashMap<String, String> queryParams =
        new MultivaluedHashMap<>();
    queryParams.put("Action", Arrays.asList(queryActions));
    when(context.getMethod()).thenReturn(HttpMethod.GET);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getQueryParameters()).thenReturn(queryParams);
    return context;
  }

  private static String read(InputStream stream) throws Exception {
    return new String(IOUtils.toByteArray(stream), StandardCharsets.UTF_8);
  }
}
