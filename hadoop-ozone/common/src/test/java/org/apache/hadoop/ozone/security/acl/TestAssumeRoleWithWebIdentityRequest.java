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

package org.apache.hadoop.ozone.security.acl;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESS_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AssumeRoleWithWebIdentityRequest}.
 */
public class TestAssumeRoleWithWebIdentityRequest {

  private static final String ROLE_ARN =
      "arn:aws:iam::123456789012:role/tomato";

  @Test
  public void testConstructorAndGetters() throws Exception {
    final Set<String> groups = set("ozone-tomato");
    final Set<String> roles = set("role:writer");
    final InetAddress ip = InetAddress.getByName("127.0.0.1");

    final AssumeRoleWithWebIdentityRequest request =
        new AssumeRoleWithWebIdentityRequest(
            "s3g.example.com", ip, "tomato-user", groups, roles, ROLE_ARN,
            "tomato-session", "https://keycloak.example.com/realms/ozone",
            "subject-1", "ozone", "keycloak", null);

    groups.add("mutated-group");
    roles.add("mutated-role");

    assertEquals(AssumeRoleWithWebIdentityRequest.ACTION,
        request.getAction());
    assertEquals("s3g.example.com", request.getHost());
    assertEquals(ip, request.getIp());
    assertEquals("tomato-user", request.getUser());
    assertEquals(set("ozone-tomato"), request.getGroups());
    assertEquals(set("role:writer"), request.getRoles());
    assertEquals(ROLE_ARN, request.getRoleArn());
    assertEquals("tomato-session", request.getRoleSessionName());
    assertEquals("https://keycloak.example.com/realms/ozone",
        request.getIssuer());
    assertEquals("subject-1", request.getSubject());
    assertEquals("ozone", request.getAudience());
    assertEquals("keycloak", request.getProviderId());
    assertEquals(null, request.getGrants());
  }

  @Test
  public void testRequiredIdentityFieldsFailClosed() {
    assertThrows(IllegalArgumentException.class,
        () -> requestWithUser(" "));
    assertThrows(IllegalArgumentException.class,
        () -> new AssumeRoleWithWebIdentityRequest(
            null, null, "tomato-user", set("ozone-tomato"), set(),
            " ", "tomato-session", "issuer", "subject", "audience",
            null, null));
  }

  @Test
  public void testAuthorizerRequestShapeAndAllow() throws Exception {
    final AssumeRoleWithWebIdentityRequest request = requestWithUser(
        "tomato-user");
    final FakeAuthorizer authorizer = FakeAuthorizer.allowing("session-policy");

    final String policy =
        authorizer.generateAssumeRoleWithWebIdentitySessionPolicy(request);

    assertEquals("session-policy", policy);
    assertEquals(request, authorizer.getLastRequest());
    assertEquals("tomato-user", authorizer.getLastRequest().getUser());
    assertEquals(set("ozone-tomato"), authorizer.getLastRequest().getGroups());
    assertEquals(AssumeRoleWithWebIdentityRequest.ACTION,
        authorizer.getLastRequest().getAction());
    assertEquals(ROLE_ARN, authorizer.getLastRequest().getRoleArn());
    assertEquals("issuer", authorizer.getLastRequest().getIssuer());
    assertEquals("subject", authorizer.getLastRequest().getSubject());
    assertEquals("audience", authorizer.getLastRequest().getAudience());
    assertEquals("session", authorizer.getLastRequest().getRoleSessionName());
    assertEquals("provider", authorizer.getLastRequest().getProviderId());
  }

  @Test
  public void testAuthorizerDenyByDefault() {
    final FakeAuthorizer authorizer = new FakeAuthorizer(false, null);

    final OMException ex = assertThrows(OMException.class,
        () -> authorizer.generateAssumeRoleWithWebIdentitySessionPolicy(
            requestWithUser("tomato-user")));

    assertEquals(ACCESS_DENIED, ex.getResult());
  }

  @Test
  public void testDefaultAuthorizerMethodIsNotSupported() {
    final IAccessAuthorizer authorizer = new IAccessAuthorizer() {
      @Override
      public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
        return false;
      }
    };

    final OMException ex = assertThrows(OMException.class,
        () -> authorizer.generateAssumeRoleWithWebIdentitySessionPolicy(
            requestWithUser("tomato-user")));

    assertEquals(NOT_SUPPORTED_OPERATION, ex.getResult());
  }

  @Test
  public void testRequestDoesNotContainTokenMaterial() {
    final String token = "eyJhbGciOiJSUzI1NiJ9.sensitive.signature";

    assertThat(requestWithUser("tomato-user").toString())
        .doesNotContain(token)
        .doesNotContain("WebIdentityToken");
  }

  private static AssumeRoleWithWebIdentityRequest requestWithUser(
      String user) {
    return new AssumeRoleWithWebIdentityRequest(
        "host", null, user, set("ozone-tomato"), set("role:writer"),
        ROLE_ARN, "session", "issuer", "subject", "audience",
        "provider", null);
  }

  private static Set<String> set(String... values) {
    return new LinkedHashSet<>(Arrays.asList(values));
  }

  private static final class FakeAuthorizer implements IAccessAuthorizer {

    private final boolean allow;
    private final String sessionPolicy;
    private AssumeRoleWithWebIdentityRequest lastRequest;

    private FakeAuthorizer(boolean allow, String sessionPolicy) {
      this.allow = allow;
      this.sessionPolicy = sessionPolicy;
    }

    private static FakeAuthorizer allowing(String sessionPolicy) {
      return new FakeAuthorizer(true, sessionPolicy);
    }

    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      return false;
    }

    @Override
    public String generateAssumeRoleWithWebIdentitySessionPolicy(
        AssumeRoleWithWebIdentityRequest request) throws OMException {
      lastRequest = request;
      if (!allow) {
        throw new OMException("Denied", ACCESS_DENIED);
      }
      return sessionPolicy;
    }

    private AssumeRoleWithWebIdentityRequest getLastRequest() {
      return lastRequest;
    }
  }
}
