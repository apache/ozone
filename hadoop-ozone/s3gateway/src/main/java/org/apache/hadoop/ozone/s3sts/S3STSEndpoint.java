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

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS STS (Security Token Service) compatible endpoint for Ozone S3 Gateway.
 * <p>
 * This endpoint provides temporary security credentials compatible with
 * AWS STS API, exposed on the port 9880 or 9881.
 * <p>
 * Currently supports only AssumeRole operation. Other STS operations will
 * return appropriate error responses.
 *
 * @see <a href="https://docs.aws.amazon.com/STS/latest/APIReference/">AWS STS API Reference</a>
 */
@Path("/")
@S3STSEnabled
public class S3STSEndpoint extends S3STSEndpointBase {

  private static final Logger LOG = LoggerFactory.getLogger(S3STSEndpoint.class);

  // STS API constants
  private static final String STS_ACTION_PARAM = "Action";
  private static final String ASSUME_ROLE_ACTION = "AssumeRole";
  private static final String ROLE_ARN_PARAM = "RoleArn";
  private static final String ROLE_DURATION_SECONDS_PARAM = "DurationSeconds";
  private static final String GET_SESSION_TOKEN_ACTION = "GetSessionToken";
  private static final String ASSUME_ROLE_WITH_SAML_ACTION = "AssumeRoleWithSAML";
  private static final String ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION = "AssumeRoleWithWebIdentity";
  private static final String GET_CALLER_IDENTITY_ACTION = "GetCallerIdentity";
  private static final String DECODE_AUTHORIZATION_MESSAGE_ACTION = "DecodeAuthorizationMessage";
  private static final String GET_ACCESS_KEY_INFO_ACTION = "GetAccessKeyInfo";

  // Default token duration (in seconds) - AWS default is 3600 (1 hour)
  private static final int DEFAULT_DURATION_SECONDS = 3600;
  private static final int MAX_DURATION_SECONDS = 43200; // 12 hours
  private static final int MIN_DURATION_SECONDS = 900;   // 15 minutes

  /**
   * STS endpoint that handles GET requests with query parameters.
   * AWS STS supports both GET and POST requests.
   *
   * @param action The STS action to perform (AssumeRole, GetSessionToken, etc.)
   * @param roleArn The ARN of the role to assume (for AssumeRole)
   * @param roleSessionName Session name for the role (for AssumeRole)
   * @param durationSeconds Duration of the token validity in seconds
   * @param version AWS STS API version (should be "2011-06-15")
   * @return Response containing STS response XML or error
   */
  @GET
  @Produces(MediaType.APPLICATION_XML)
  public Response get(
      @QueryParam("Action") String action,
      @QueryParam("RoleArn") String roleArn,
      @QueryParam("RoleSessionName") String roleSessionName,
      @QueryParam("DurationSeconds") Integer durationSeconds,
      @QueryParam("Version") String version) throws OS3Exception {

    return handleSTSRequest(action, roleArn, roleSessionName, durationSeconds, version);
  }

  /**
   * STS endpoint that handles POST requests with form data.
   * AWS STS typically uses POST requests with form-encoded parameters.
   *
   * @param action The STS action to perform
   * @param roleArn The ARN of the role to assume
   * @param roleSessionName Session name for the role
   * @param durationSeconds Duration of the token validity
   * @param version AWS STS API version
   * @return Response containing STS response XML or error
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public Response post(
      @FormParam("Action") String action,
      @FormParam("RoleArn") String roleArn,
      @FormParam("RoleSessionName") String roleSessionName,
      @FormParam("DurationSeconds") Integer durationSeconds,
      @FormParam("Version") String version) throws OS3Exception {

    return handleSTSRequest(action, roleArn, roleSessionName, durationSeconds, version);
  }

  private Response handleSTSRequest(String action, String roleArn, String roleSessionName,
      Integer durationSeconds, String version) throws OS3Exception {
    try {
      if (action == null) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Missing required parameter: " + STS_ACTION_PARAM)
            .build();
      }
      int duration;
      try {
        duration = validateDuration(durationSeconds);
      } catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(e.getMessage())
            .build();
      }

      switch (action) {
      case ASSUME_ROLE_ACTION:
        return handleAssumeRole(roleArn, roleSessionName, duration);
      // These operations are not supported yet
      case GET_SESSION_TOKEN_ACTION:
      case ASSUME_ROLE_WITH_SAML_ACTION:
      case ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION:
      case GET_CALLER_IDENTITY_ACTION:
      case DECODE_AUTHORIZATION_MESSAGE_ACTION:
      case GET_ACCESS_KEY_INFO_ACTION:
        return Response.status(Response.Status.NOT_IMPLEMENTED)
            .entity("Operation " + action + " is not supported yet.")
            .build();
      default:
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Unsupported Action: " + action)
            .build();
      }
    } catch (OS3Exception s3e) {
      // Handle known S3 exceptions
      LOG.error("S3 Error during STS request: {}", s3e.toXml());
      throw s3e;
    } catch (Exception ex) {
      LOG.error("Unexpected error during STS request", ex);
      return Response.serverError().build();
    }
  }

  private int validateDuration(Integer durationSeconds) throws IllegalArgumentException, OS3Exception {
    if (durationSeconds == null) {
      return DEFAULT_DURATION_SECONDS;
    }

    if (durationSeconds < MIN_DURATION_SECONDS || durationSeconds > MAX_DURATION_SECONDS) {
      throw new IllegalArgumentException(
          "Invalid Value: " + ROLE_DURATION_SECONDS_PARAM + " must be between " + MIN_DURATION_SECONDS +
              " and " + MAX_DURATION_SECONDS + " seconds");
    }

    return durationSeconds;
  }

  private Response handleAssumeRole(String roleArn, String roleSessionName, int duration)
      throws IOException, OS3Exception {
    // Validate required parameters for AssumeRole. RoleArn is required to pass the
    if (roleArn == null || roleArn.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Missing required parameter: " + ROLE_ARN_PARAM)
          .build();
    }

    if (roleSessionName == null || roleSessionName.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Missing required parameter: RoleSessionName")
          .build();
    }

    // Validate role session name format (AWS requirements)
    if (!isValidRoleSessionName(roleSessionName)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid RoleSessionName: must be 2-64 characters long and " +
              "contain only alphanumeric characters, +, =, ,, ., @, -")
          .build();
    }
    // TODO: Add a validation if a user is not an admin but still allowed to call AssumeRole
    // TODO: Convert roleArn to a valid Ozone ACL
    // TODO: Validate requested ACLs
    // TODO: Create a new S3 credentials for this role session
    // TODO: Add validated ACLs for the new credentials
    // TODO: How do we handle expired credentials? We don't support renewal?
    // String dummyCredentials = getClient().getObjectStore().getS3StsToken(userNameFromRequest());
    // Generate AssumeRole response
    String responseXml = generateAssumeRoleResponse(roleArn, roleSessionName, duration);

    return Response.ok(responseXml)
        .header("Content-Type", "text/xml")
        .build();
  }

  // TODO: implement private List<OzoneAcl> toOzoneAcls(String roleArn) to convert roleArn to Ozone ACLs
  // TODO: implement private List<OzoneAcl> checkAclSubset(List<OzoneAcl> requestedAcls) to validate requested ACLs

  private boolean isValidRoleSessionName(String roleSessionName) {
    if (roleSessionName.length() < 2 || roleSessionName.length() > 64) {
      return false;
    }

    // AWS allows: alphanumeric, +, =, ,, ., @, -
    return roleSessionName.matches("[a-zA-Z0-9+=,.@\\-]+");
  }

  // TODO: replace mock implementation with actual logic to generate new credentials
  private String generateAssumeRoleResponse(String roleArn, String roleSessionName, int duration) {
    // Generate realistic-looking temporary credentials
    String accessKeyId = "ASIA" + generateRandomAlphanumeric(16); // AWS temp keys start with ASIA
    String secretAccessKey = generateRandomBase64(40);
    String sessionToken = generateSessionToken();
    String expiration = getExpirationTime(duration);

    // Generate AssumedRoleId (format: AROLEID:RoleSessionName)
    String roleId = "AROA" + generateRandomAlphanumeric(16);
    String assumedRoleId = roleId + ":" + roleSessionName;

    String requestId = UUID.randomUUID().toString();

    return String.format(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n" +
            "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">%n" +
            "  <AssumeRoleResult>%n" +
            "    <Credentials>%n" +
            "      <AccessKeyId>%s</AccessKeyId>%n" +
            "      <SecretAccessKey>%s</SecretAccessKey>%n" +
            "      <SessionToken>%s</SessionToken>%n" +
            "      <Expiration>%s</Expiration>%n" +
            "    </Credentials>%n" +
            "    <AssumedRoleUser>%n" +
            "      <AssumedRoleId>%s</AssumedRoleId>%n" +
            "      <Arn>%s</Arn>%n" +
            "    </AssumedRoleUser>%n" +
            "  </AssumeRoleResult>%n" +
            "  <ResponseMetadata>%n" +
            "    <RequestId>%s</RequestId>%n" +
            "  </ResponseMetadata>%n" +
            "</AssumeRoleResponse>",
        accessKeyId, secretAccessKey, sessionToken, expiration,
        assumedRoleId, roleArn, requestId);
  }

  // Helper methods to generate random alphanumeric and base64 strings for mock credentials.
  // TODO: these should be replaced with actual credential generation logic.
  private String generateRandomAlphanumeric(int length) {
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
  }

  private String generateRandomBase64(int length) {
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt((random.nextInt(chars.length()))));
    }
    return sb.toString();
  }

  private String generateSessionToken() {
    byte[] tokenBytes = new byte[128];
    Random random = new Random();
    for (int i = 0; i < tokenBytes.length; i++) {
      tokenBytes[i] = (byte) random.nextInt(256);
    }
    return Base64.getEncoder().encodeToString(tokenBytes);
  }

  private String getExpirationTime(int durationSeconds) {
    Instant expiration = Instant.now().plusSeconds(durationSeconds);
    return DateTimeFormatter.ISO_INSTANT.format(expiration);
  }
}
