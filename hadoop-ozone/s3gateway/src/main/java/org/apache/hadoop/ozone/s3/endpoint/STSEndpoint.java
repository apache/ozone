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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS STS (Security Token Service) compatible endpoint for Ozone S3 Gateway.
 * 
 * This endpoint provides temporary security credentials compatible with
 * AWS STS API. This is an experimental implementation with dummy responses
 * that conform to the AWS STS API contract.
 * 
 * @see <a href="https://docs.aws.amazon.com/STS/latest/APIReference/">AWS STS API Reference</a>
 */
@Path("/")
public class STSEndpoint extends EndpointBase {

  private static final Logger LOG = LoggerFactory.getLogger(STSEndpoint.class);
  
  // STS API constants
  private static final String STS_ACTION_PARAM = "Action";
  private static final String ASSUME_ROLE_ACTION = "AssumeRole";
  private static final String GET_SESSION_TOKEN_ACTION = "GetSessionToken";
  private static final String ASSUME_ROLE_WITH_SAML_ACTION = "AssumeRoleWithSAML";
  private static final String ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION = "AssumeRoleWithWebIdentity";
  
  // Default token duration (in seconds) - AWS default is 3600 (1 hour)
  private static final int DEFAULT_DURATION_SECONDS = 3600;
  private static final int MAX_DURATION_SECONDS = 43200; // 12 hours
  private static final int MIN_DURATION_SECONDS = 900;   // 15 minutes

  /**
   * STS endpoint that handles both GET and POST requests.
   * AWS STS supports both GET (with query parameters) and POST (with form data).
   * 
   * @param action The STS action to perform (AssumeRole, GetSessionToken, etc.)
   * @param roleArn The ARN of the role to assume (for AssumeRole)
   * @param roleSessionName Session name for the role (for AssumeRole)
   * @param durationSeconds Duration of the token validity in seconds
   * @param version AWS STS API version (should be "2011-06-15")
   * @param httpHeaders HTTP headers for the request
   * @return Response containing STS response XML
   * @throws OS3Exception if there's an error processing the request
   * @throws IOException if there's an I/O error
   */
  @GET
  @Produces(MediaType.APPLICATION_XML)
  public Response handleSTSGet(
      @QueryParam("Action") String action,
      @QueryParam("RoleArn") String roleArn,
      @QueryParam("RoleSessionName") String roleSessionName,
      @QueryParam("DurationSeconds") Integer durationSeconds,
      @QueryParam("Version") String version,
      @Context HttpHeaders httpHeaders) throws OS3Exception, IOException {
    
    return handleSTSRequest(action, roleArn, roleSessionName, durationSeconds, version);
  }

  /**
   * STS endpoint that handles POST requests with form data.
   * 
   * @param action The STS action to perform
   * @param roleArn The ARN of the role to assume
   * @param roleSessionName Session name for the role
   * @param durationSeconds Duration of the token validity
   * @param version AWS STS API version
   * @param httpHeaders HTTP headers for the request
   * @return Response containing STS response XML
   * @throws OS3Exception if there's an error processing the request
   * @throws IOException if there's an I/O error
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public Response handleSTSPost(
      @QueryParam("Action") String action,
      @QueryParam("RoleArn") String roleArn,
      @QueryParam("RoleSessionName") String roleSessionName,
      @QueryParam("DurationSeconds") Integer durationSeconds,
      @QueryParam("Version") String version,
      @Context HttpHeaders httpHeaders) throws OS3Exception, IOException {
    
    return handleSTSRequest(action, roleArn, roleSessionName, durationSeconds, version);
  }

  private Response handleSTSRequest(String action, String roleArn, String roleSessionName,
      Integer durationSeconds, String version) throws OS3Exception, IOException {
    
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.STS_GET_SESSION_TOKEN;
    
    try {
      // Validate version
      if (version != null && !"2011-06-15".equals(version)) {
        throw newError(S3ErrorTable.INVALID_REQUEST, "Unsupported STS API version: " + version);
      }
      
      // Default action if not specified
      if (action == null) {
        action = GET_SESSION_TOKEN_ACTION;
      }
      
      // Validate duration
      int duration = validateDuration(durationSeconds);
      
      String responseXml;
      switch (action) {
        case ASSUME_ROLE_ACTION:
          s3GAction = S3GAction.STS_ASSUME_ROLE;
          responseXml = handleAssumeRole(roleArn, roleSessionName, duration);
          break;
        case GET_SESSION_TOKEN_ACTION:
          s3GAction = S3GAction.STS_GET_SESSION_TOKEN;
          responseXml = handleGetSessionToken(duration);
          break;
        case ASSUME_ROLE_WITH_SAML_ACTION: // handle that it is not supported
          s3GAction = S3GAction.STS_ASSUME_ROLE_WITH_SAML;
          responseXml = handleAssumeRoleWithSAML(roleArn, roleSessionName, duration);
          break;
        case ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION:
          s3GAction = S3GAction.STS_ASSUME_ROLE_WITH_WEB_IDENTITY;
          responseXml = handleAssumeRoleWithWebIdentity(roleArn, roleSessionName, duration);
          break;
        default:
          throw newError(S3ErrorTable.INVALID_REQUEST, "Unsupported STS action: " + action);
      }
      
      AUDIT.logReadSuccess(
          buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
      getMetrics().updateSTSSuccessStats(startNanos);
      
      return Response.ok(responseXml)
          .header("Content-Type", "text/xml")
          .build();
          
    } catch (Exception ex) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      getMetrics().updateSTSFailureStats(startNanos);
      throw ex;
    }
  }

  private int validateDuration(Integer durationSeconds) throws OS3Exception {
    if (durationSeconds == null) {
      return DEFAULT_DURATION_SECONDS;
    }
    
    if (durationSeconds < MIN_DURATION_SECONDS || durationSeconds > MAX_DURATION_SECONDS) {
      throw newError(S3ErrorTable.INVALID_REQUEST, 
          "DurationSeconds must be between " + MIN_DURATION_SECONDS + 
          " and " + MAX_DURATION_SECONDS + " seconds");
    }
    
    return durationSeconds;
  }

  private String handleAssumeRole(String roleArn, String roleSessionName, int duration) 
      throws OS3Exception {
    
    if (roleArn == null || roleArn.trim().isEmpty()) {
      throw newError(S3ErrorTable.INVALID_REQUEST, "RoleArn parameter is required for AssumeRole");
    }
    
    if (roleSessionName == null || roleSessionName.trim().isEmpty()) {
      throw newError(S3ErrorTable.INVALID_REQUEST, 
          "RoleSessionName parameter is required for AssumeRole");
    }
    
    return generateAssumeRoleResponse(roleArn, roleSessionName, duration);
  }

  private String handleGetSessionToken(int duration) {
    return generateGetSessionTokenResponse(duration);
  }

  private String handleAssumeRoleWithSAML(String roleArn, String roleSessionName, int duration) 
      throws OS3Exception {
    
    if (roleArn == null || roleArn.trim().isEmpty()) {
      throw newError(S3ErrorTable.INVALID_REQUEST, 
          "RoleArn parameter is required for AssumeRoleWithSAML");
    }
    
    return generateAssumeRoleResponse(roleArn, 
        roleSessionName != null ? roleSessionName : "SAMLSession", duration);
  }

  private String handleAssumeRoleWithWebIdentity(String roleArn, String roleSessionName, 
      int duration) throws OS3Exception {
    
    if (roleArn == null || roleArn.trim().isEmpty()) {
      throw newError(S3ErrorTable.INVALID_REQUEST, 
          "RoleArn parameter is required for AssumeRoleWithWebIdentity");
    }
    
    return generateAssumeRoleResponse(roleArn, 
        roleSessionName != null ? roleSessionName : "WebIdentitySession", duration);
  }

  private String generateAssumeRoleResponse(String roleArn, String roleSessionName, int duration) {
    String accessKeyId = "ASIAI" + generateRandomString(15);
    String secretAccessKey = generateRandomString(40);
    String sessionToken = generateSessionToken();
    String expiration = getExpirationTime(duration);
    String assumedRoleId = "AROLEID" + generateRandomString(15) + ":" + roleSessionName;
    String requestId = UUID.randomUUID().toString();
    
    return String.format(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n" +
        "  <AssumeRoleResult>\n" +
        "    <Credentials>\n" +
        "      <AccessKeyId>%s</AccessKeyId>\n" +
        "      <SecretAccessKey>%s</SecretAccessKey>\n" +
        "      <SessionToken>%s</SessionToken>\n" +
        "      <Expiration>%s</Expiration>\n" +
        "    </Credentials>\n" +
        "    <AssumedRoleUser>\n" +
        "      <AssumedRoleId>%s</AssumedRoleId>\n" +
        "      <Arn>%s</Arn>\n" +
        "    </AssumedRoleUser>\n" +
        "  </AssumeRoleResult>\n" +
        "  <ResponseMetadata>\n" +
        "    <RequestId>%s</RequestId>\n" +
        "  </ResponseMetadata>\n" +
        "</AssumeRoleResponse>",
        accessKeyId, secretAccessKey, sessionToken, expiration,
        assumedRoleId, roleArn, requestId);
  }

  private String generateGetSessionTokenResponse(int duration) {
    String accessKeyId = "ASIAI" + generateRandomString(15);
    String secretAccessKey = generateRandomString(40);
    String sessionToken = generateSessionToken();
    String expiration = getExpirationTime(duration);
    String requestId = UUID.randomUUID().toString();
    
    return String.format(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<GetSessionTokenResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n" +
        "  <GetSessionTokenResult>\n" +
        "    <Credentials>\n" +
        "      <AccessKeyId>%s</AccessKeyId>\n" +
        "      <SecretAccessKey>%s</SecretAccessKey>\n" +
        "      <SessionToken>%s</SessionToken>\n" +
        "      <Expiration>%s</Expiration>\n" +
        "    </Credentials>\n" +
        "  </GetSessionTokenResult>\n" +
        "  <ResponseMetadata>\n" +
        "    <RequestId>%s</RequestId>\n" +
        "  </ResponseMetadata>\n" +
        "</GetSessionTokenResponse>",
        accessKeyId, secretAccessKey, sessionToken, expiration, requestId);
  }

  private String generateRandomString(int length) {
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt((int) (Math.random() * chars.length())));
    }
    return sb.toString();
  }

  private String generateSessionToken() {
    // Generate a realistic-looking session token (base64 encoded)
    byte[] tokenBytes = new byte[96]; // 128 characters when base64 encoded
    for (int i = 0; i < tokenBytes.length; i++) {
      tokenBytes[i] = (byte) (Math.random() * 256);
    }
    return Base64.getEncoder().encodeToString(tokenBytes);
  }

  private String getExpirationTime(int durationSeconds) {
    Instant expiration = Instant.now().plusSeconds(durationSeconds);
    return DateTimeFormatter.ISO_INSTANT.format(expiration);
  }

  @Override
  public void init() {
    // No special initialization needed
  }
}
