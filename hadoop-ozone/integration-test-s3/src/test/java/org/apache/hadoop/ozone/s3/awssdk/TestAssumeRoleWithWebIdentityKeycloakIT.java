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

package org.apache.hadoop.ozone.s3.awssdk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.ozone.security.acl.AssumeRoleWithWebIdentityRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Integration coverage for a real Keycloak-issued JWT with Ozone STS
 * AssumeRoleWithWebIdentity.
 */
class TestAssumeRoleWithWebIdentityKeycloakIT
    extends AbstractAssumeRoleWithWebIdentityS3Test {

  private static final DockerImageName KEYCLOAK_IMAGE =
      DockerImageName.parse("quay.io/keycloak/keycloak:26.0.7");
  private static final Pattern ACCESS_TOKEN_PATTERN =
      Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
  private static GenericContainer<?> keycloak;

  @Override
  protected String issuerUri() {
    return keycloakIssuerUri();
  }

  @Override
  protected String jwksUri() {
    return keycloakIssuerUri() + "/protocol/openid-connect/certs";
  }

  @AfterAll
  static void stopKeycloak() {
    if (keycloak != null) {
      keycloak.stop();
      keycloak = null;
    }
  }

  @Test
  void keycloakTokenCanAssumeRoleAndUseTemporaryS3Credentials()
      throws Exception {
    String token = fetchToken("ozone-sts", "tomato-user", "tomato-password");

    StsCredentials credentials =
        assumeRoleWithWebIdentity(token, keycloakSubject(token));

    AssumeRoleWithWebIdentityRequest request = lastAssumeRoleRequest();
    assertThat(request).isNotNull();
    assertThat(request.getUser()).isEqualTo("tomato-user");
    assertThat(request.getGroups()).contains("ozone-tomato");
    assertThat(request.getIssuer()).isEqualTo(keycloakIssuerUri());
    assertThat(request.getAudience()).isEqualTo(AUDIENCE);
    assertThat(request.getRoleArn()).isEqualTo(ROLE_ARN);
    assertThat(request.getRoleSessionName()).isEqualTo(SESSION_NAME);
    assertThat(request.getProviderId()).isEqualTo(PROVIDER_ID);

    assertTemporaryCredentialsAuthorizeS3Operations(credentials);
    assertThat(accessChecksWithSessionPolicy()).isPositive();
  }

  @Test
  void deniedKeycloakUserCannotAssumeRole() throws Exception {
    String token = fetchToken("ozone-sts", "denied-user", "denied-password");

    assertStsFails(token, 403);
  }

  @Test
  void wrongAudienceKeycloakTokenFails() throws Exception {
    String token = fetchToken("wrong-audience-client", "tomato-user",
        "tomato-password");

    assertStsFails(token, 403);
  }

  @Test
  void tamperedKeycloakTokenFails() throws Exception {
    String token = fetchToken("ozone-sts", "tomato-user", "tomato-password");

    assertStsFails(rewriteJwtPayload(token, "ozone-tomato", "ozone-admins"),
        403);
  }

  @Test
  void missingWebIdentityTokenFails() throws Exception {
    HttpResponse response = postSts(null);

    assertThat(response.getCode()).isGreaterThanOrEqualTo(400);
  }

  private static synchronized String keycloakIssuerUri() {
    GenericContainer<?> container = keycloak();
    return "http://" + container.getHost() + ":"
        + container.getMappedPort(8080) + "/realms/ozone-test";
  }

  private static synchronized GenericContainer<?> keycloak() {
    if (keycloak == null) {
      keycloak = new GenericContainer<>(KEYCLOAK_IMAGE)
          .withExposedPorts(8080)
          .withEnv("KEYCLOAK_ADMIN", "admin")
          .withEnv("KEYCLOAK_ADMIN_PASSWORD", "admin")
          .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
          .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource(
                  "keycloak/ozone-test-realm.json"),
              "/opt/keycloak/data/import/ozone-test-realm.json")
          .withCommand("start-dev", "--import-realm",
              "--hostname-strict=false")
          .waitingFor(Wait.forHttp(
              "/realms/ozone-test/.well-known/openid-configuration")
              .forStatusCode(200));
      keycloak.start();
    }
    return keycloak;
  }

  private static String fetchToken(String clientId, String username,
      String password) throws IOException {
    URL url = URI.create(keycloakIssuerUri()
        + "/protocol/openid-connect/token").toURL();
    String body = "grant_type=password"
        + "&client_id=" + encode(clientId)
        + "&username=" + encode(username)
        + "&password=" + encode(password);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type",
        "application/x-www-form-urlencoded");
    try (OutputStream output = connection.getOutputStream()) {
      output.write(body.getBytes(UTF_8));
    }
    int code = connection.getResponseCode();
    String response = code >= 400
        ? IOUtils.toString(connection.getErrorStream(), UTF_8)
        : IOUtils.toString(connection.getInputStream(), UTF_8);
    assertThat(code).as("Keycloak token endpoint response code")
        .isEqualTo(200);
    Matcher matcher = ACCESS_TOKEN_PATTERN.matcher(response);
    assertThat(matcher.find()).as("Keycloak access token is present")
        .isTrue();
    return matcher.group(1);
  }

  private static String keycloakSubject(String jwt) {
    return jsonValue(jwtPayload(jwt), "sub");
  }

  private static String rewriteJwtPayload(String jwt, String from, String to) {
    String[] parts = jwt.split("\\.");
    assertThat(parts).hasSize(3);
    String payload = jwtPayload(jwt);
    assertThat(payload).contains(from);
    parts[1] = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(payload.replace(from, to).getBytes(UTF_8));
    return String.join(".", parts);
  }

  private static String jwtPayload(String jwt) {
    String[] parts = jwt.split("\\.");
    assertThat(parts).hasSize(3);
    return new String(Base64.getUrlDecoder().decode(parts[1]), UTF_8);
  }

  private static String jsonValue(String json, String field) {
    Pattern pattern = Pattern.compile("\"" + Pattern.quote(field)
        + "\"\\s*:\\s*\"([^\"]+)\"");
    Matcher matcher = pattern.matcher(json);
    assertThat(matcher.find()).as("JWT claim %s is present", field).isTrue();
    return matcher.group(1);
  }

  private static String encode(String value) throws IOException {
    return URLEncoder.encode(value, UTF_8.name());
  }
}
