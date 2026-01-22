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
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.AssumeRoleResponseInfo;
import org.apache.hadoop.ozone.om.helpers.AwsRoleArnValidator;
import org.apache.hadoop.ozone.om.helpers.S3STSUtils;
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
  private static final String GET_SESSION_TOKEN_ACTION = "GetSessionToken";
  private static final String ASSUME_ROLE_WITH_SAML_ACTION = "AssumeRoleWithSAML";
  private static final String ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION = "AssumeRoleWithWebIdentity";
  private static final String GET_CALLER_IDENTITY_ACTION = "GetCallerIdentity";
  private static final String DECODE_AUTHORIZATION_MESSAGE_ACTION = "DecodeAuthorizationMessage";
  private static final String GET_ACCESS_KEY_INFO_ACTION = "GetAccessKeyInfo";

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
      @QueryParam("Version") String version,
      @QueryParam("Policy") String awsIamSessionPolicy) throws OS3Exception {

    return handleSTSRequest(action, roleArn, roleSessionName, durationSeconds, version, awsIamSessionPolicy);
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
      @FormParam("Version") String version,
      @FormParam("Policy") String awsIamSessionPolicy) throws OS3Exception {

    return handleSTSRequest(action, roleArn, roleSessionName, durationSeconds, version, awsIamSessionPolicy);
  }

  private Response handleSTSRequest(String action, String roleArn, String roleSessionName,
      Integer durationSeconds, String version, String awsIamSessionPolicy) throws OS3Exception {
    try {
      if (action == null) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Missing required parameter: " + STS_ACTION_PARAM)
            .build();
      }
      if (version == null || !version.equals("2011-06-15")) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Invalid or missing Version parameter. Supported version is 2011-06-15.")
            .build();
      }

      switch (action) {
      case ASSUME_ROLE_ACTION:
        return handleAssumeRole(roleArn, roleSessionName, durationSeconds, awsIamSessionPolicy);
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

  private Response handleAssumeRole(String roleArn, String roleSessionName, Integer durationSeconds,
      String awsIamSessionPolicy) throws IOException, OS3Exception {
    // Validate parameters
    int duration;
    try {
      duration = S3STSUtils.validateDuration(durationSeconds);
      AwsRoleArnValidator.validateAndExtractRoleNameFromArn(roleArn);
      S3STSUtils.validateRoleSessionName(roleSessionName);
      S3STSUtils.validateSessionPolicy(awsIamSessionPolicy);
    } catch (OMException e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(e.getMessage())
          .build();
    }

    final String assumedRoleUserArn;
    try {
      assumedRoleUserArn = S3STSUtils.toAssumedRoleUserArn(roleArn, roleSessionName);
    } catch (OMException e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(e.getMessage())
          .build();
    }

    final AssumeRoleResponseInfo responseInfo = getClient()
        .getObjectStore()
        .assumeRole(roleArn, roleSessionName, duration, awsIamSessionPolicy);
    // Generate AssumeRole response
    final String responseXml = generateAssumeRoleResponse(assumedRoleUserArn, responseInfo);
    return Response.ok(responseXml)
        .header("Content-Type", "text/xml")
        .build();
  }

  private String generateAssumeRoleResponse(String assumedRoleUserArn, AssumeRoleResponseInfo responseInfo)
      throws IOException {
    final String accessKeyId = responseInfo.getAccessKeyId();
    final String secretAccessKey = responseInfo.getSecretAccessKey();
    final String sessionToken = responseInfo.getSessionToken();
    final String assumedRoleId = responseInfo.getAssumedRoleId();

    final String expiration = DateTimeFormatter.ISO_INSTANT.format(
        Instant.ofEpochSecond(responseInfo.getExpirationEpochSeconds()).atOffset(ZoneOffset.UTC).toInstant());

    final String requestId = UUID.randomUUID().toString();

    try {
      final S3AssumeRoleResponseXml response = new S3AssumeRoleResponseXml();
      final S3AssumeRoleResponseXml.AssumeRoleResult result = new S3AssumeRoleResponseXml.AssumeRoleResult();
      final S3AssumeRoleResponseXml.Credentials credentials = new S3AssumeRoleResponseXml.Credentials();
      credentials.setAccessKeyId(accessKeyId);
      credentials.setSecretAccessKey(secretAccessKey);
      credentials.setSessionToken(sessionToken);
      credentials.setExpiration(expiration);
      result.setCredentials(credentials);
      final S3AssumeRoleResponseXml.AssumedRoleUser user = new S3AssumeRoleResponseXml.AssumedRoleUser();
      user.setAssumedRoleId(assumedRoleId);
      user.setArn(assumedRoleUserArn);
      result.setAssumedRoleUser(user);
      response.setAssumeRoleResult(result);
      final S3AssumeRoleResponseXml.ResponseMetadata meta = new S3AssumeRoleResponseXml.ResponseMetadata();
      meta.setRequestId(requestId);
      response.setResponseMetadata(meta);

      final JAXBContext jaxbContext = JAXBContext.newInstance(S3AssumeRoleResponseXml.class);
      final Marshaller marshaller = jaxbContext.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      final StringWriter stringWriter = new StringWriter();
      marshaller.marshal(response, stringWriter);
      return stringWriter.toString();
    } catch (JAXBException e) {
      throw new IOException("Failed to marshal AssumeRole response", e);
    }
  }
}
