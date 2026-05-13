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

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_INTERNAL_FAILURE;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_INVALID_ACTION;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_INVALID_ACTION_NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_INVALID_CLIENT_TOKEN_ID;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_MALFORMED_POLICY_DOCUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_UNSUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_VALIDATION_ERROR;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.AssumeRoleResponseInfo;
import org.apache.hadoop.ozone.om.helpers.AwsRoleArnValidator;
import org.apache.hadoop.ozone.om.helpers.S3STSUtils;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.OSTSException;
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
  private static final String ASSUME_ROLE_ACTION = "AssumeRole";
  private static final String GET_SESSION_TOKEN_ACTION = "GetSessionToken";
  private static final String ASSUME_ROLE_WITH_SAML_ACTION = "AssumeRoleWithSAML";
  private static final String ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION = "AssumeRoleWithWebIdentity";
  private static final String GET_CALLER_IDENTITY_ACTION = "GetCallerIdentity";
  private static final String DECODE_AUTHORIZATION_MESSAGE_ACTION = "DecodeAuthorizationMessage";
  private static final String GET_ACCESS_KEY_INFO_ACTION = "GetAccessKeyInfo";

  private static final String EXPECTED_VERSION = "2011-06-15";

  // JAXBContext is relatively expensive to create and is threadsafe, so cache and reuse
  private static final JAXBContext JAXB_CONTEXT;

  static {
    try {
      JAXB_CONTEXT = JAXBContext.newInstance(S3AssumeRoleResponseXml.class);
    } catch (JAXBException e) {
      throw new RuntimeException("Failed to initialize JAXBContext: " + e, e);
    }
  }

  @Inject
  private RequestIdentifier requestIdentifier;

  @VisibleForTesting
  public void setRequestIdentifier(RequestIdentifier requestIdentifier) {
    this.requestIdentifier = requestIdentifier;
  }

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
    final String requestId = requestIdentifier.getRequestId();
    // NOTE: invalid, missing or unsupported actions are not added to the audit log
    try {
      if (action == null) {
        // Amazon STS has a different structure for the XML error response when the action is missing
        return Response.status(BAD_REQUEST)
            .entity("<UnknownOperationException/>")
            .type(MediaType.APPLICATION_XML)
            .build();
      }

      switch (action) {
      case ASSUME_ROLE_ACTION:
        return handleAssumeRole(roleArn, roleSessionName, durationSeconds, awsIamSessionPolicy, version, requestId);
      // These operations are not supported yet
      case GET_SESSION_TOKEN_ACTION:
      case ASSUME_ROLE_WITH_SAML_ACTION:
      case ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION:
      case GET_CALLER_IDENTITY_ACTION:
      case DECODE_AUTHORIZATION_MESSAGE_ACTION:
      case GET_ACCESS_KEY_INFO_ACTION:
        throw new OSTSException(STS_INVALID_ACTION_NOT_IMPLEMENTED)
            .withMessage("Operation " + action + " is not supported yet.");
      default:
        throw new OSTSException(STS_INVALID_ACTION)
            .withMessage("Could not find operation " + action + " for version " +
                (version == null ? "NO_VERSION_SPECIFIED.  Expected version is: " + EXPECTED_VERSION : version));
      }
    } catch (OSTSException e) {
      throw e;
    } catch (Exception ex) {
      LOG.error("Unexpected error during STS request", ex);
      throw new OSTSException(STS_INTERNAL_FAILURE, ex).withType("Receiver");
    }
  }

  private Response handleAssumeRole(String roleArn, String roleSessionName, Integer durationSeconds,
      String awsIamSessionPolicy, String version, String requestId) throws OSTSException {
    final String action = "AssumeRole";
    final Map<String, String> auditParams = getAuditParameters();
    S3STSUtils.addAssumeRoleAuditParams(
        auditParams, roleArn, roleSessionName, awsIamSessionPolicy,
        durationSeconds == null ? S3STSUtils.DEFAULT_DURATION_SECONDS : durationSeconds,
        requestId);

    // Validate parameters
    if (version == null || !version.equals(EXPECTED_VERSION)) {
      final OSTSException exception = new OSTSException(STS_INVALID_ACTION)
          .withMessage("Could not find operation " + action + " for version " +
              (version == null ? "NO_VERSION_SPECIFIED.  Expected version is: " + EXPECTED_VERSION : version));
      getAuditLogger().logWriteFailure(buildAuditMessageForFailure(S3GAction.ASSUME_ROLE, auditParams, exception));
      throw exception;
    }

    final Set<String> validationErrors = new HashSet<>();
    int duration = durationSeconds == null ? S3STSUtils.DEFAULT_DURATION_SECONDS : durationSeconds;
    try {
      duration = S3STSUtils.validateDuration(durationSeconds);
    } catch (OMException e) {
      validationErrors.add(e.getMessage());
    }

    try {
      AwsRoleArnValidator.validateAndExtractRoleNameFromArn(roleArn);
    } catch (OMException e) {
      validationErrors.add(e.getMessage());
    }

    try {
      S3STSUtils.validateRoleSessionName(roleSessionName);
    } catch (OMException e) {
      validationErrors.add(e.getMessage());
    }

    try {
      if (LOG.isDebugEnabled() && StringUtils.isNotEmpty(awsIamSessionPolicy)) {
        LOG.debug(
            "AssumeRole requestId={} received Policy(len={}): {}", requestId, awsIamSessionPolicy.length(),
            awsIamSessionPolicy);
      }
      S3STSUtils.validateSessionPolicy(awsIamSessionPolicy);
    } catch (OMException e) {
      validationErrors.add(e.getMessage());
    }

    final int numValidationErrors = validationErrors.size();
    if (numValidationErrors > 0) {
      //noinspection StringBufferReplaceableByString
      final StringBuilder builder = new StringBuilder()
          .append(numValidationErrors)
          .append(" validation ")
          .append(numValidationErrors > 1 ? "errors detected: " : "error detected: ")
          .append(String.join(";", validationErrors));
      final String validationMessage = builder.toString();
      final OSTSException exception = new OSTSException(STS_VALIDATION_ERROR).withMessage(validationMessage);
      getAuditLogger().logWriteFailure(buildAuditMessageForFailure(S3GAction.ASSUME_ROLE, auditParams, exception));
      throw exception;
    }

    final String assumedRoleUserArn = S3STSUtils.toAssumedRoleUserArn(roleArn, roleSessionName);
    try {
      final AssumeRoleResponseInfo responseInfo = getClient()
          .getObjectStore()
          .assumeRole(roleArn, roleSessionName, duration, awsIamSessionPolicy, requestId);
      // Generate AssumeRole response
      final String responseXml = generateAssumeRoleResponse(assumedRoleUserArn, responseInfo, requestId);

      getAuditLogger().logWriteSuccess(buildAuditMessageForSuccess(S3GAction.ASSUME_ROLE, auditParams));

      return Response.ok(responseXml)
          .header("Content-Type", "text/xml")
          .build();
    } catch (IOException e) {
      LOG.error("Error during AssumeRole processing", e);

      getAuditLogger().logWriteFailure(buildAuditMessageForFailure(S3GAction.ASSUME_ROLE, auditParams, e));

      if (e instanceof OMException) {
        final OMException omException = (OMException) e;
        if (omException.getResult() == OMException.ResultCodes.ACCESS_DENIED ||
            omException.getResult() == OMException.ResultCodes.PERMISSION_DENIED ||
            omException.getResult() == OMException.ResultCodes.TOKEN_EXPIRED) {
          throw new OSTSException(ACCESS_DENIED)
              .withMessage("User is not authorized to perform: sts:AssumeRole on resource: " + roleArn);
        }
        if (omException.getResult() == OMException.ResultCodes.INVALID_TOKEN) {
          throw new OSTSException(STS_INVALID_CLIENT_TOKEN_ID);
        }
        if (omException.getResult() == OMException.ResultCodes.NOT_SUPPORTED_OPERATION ||
            omException.getResult() == OMException.ResultCodes.FEATURE_NOT_ENABLED) {
          throw new OSTSException(STS_UNSUPPORTED_OPERATION).withMessage(omException.getMessage());
        }
        if (omException.getResult() == OMException.ResultCodes.INVALID_REQUEST) {
          throw new OSTSException(STS_VALIDATION_ERROR).withMessage(omException.getMessage());
        }
        if (omException.getResult() == OMException.ResultCodes.MALFORMED_POLICY_DOCUMENT) {
          throw new OSTSException(STS_MALFORMED_POLICY_DOCUMENT).withMessage(omException.getMessage());
        }
      }
      throw new OSTSException(STS_INTERNAL_FAILURE, e).withType("Receiver");
    } catch (Exception e) {
      getAuditLogger().logWriteFailure(buildAuditMessageForFailure(S3GAction.ASSUME_ROLE, auditParams, e));
      throw e;
    }
  }

  private String generateAssumeRoleResponse(String assumedRoleUserArn, AssumeRoleResponseInfo responseInfo,
      String requestId) throws IOException {
    final String accessKeyId = responseInfo.getAccessKeyId();
    final String secretAccessKey = responseInfo.getSecretAccessKey();
    final String sessionToken = responseInfo.getSessionToken();
    final String assumedRoleId = responseInfo.getAssumedRoleId();

    final String expiration = DateTimeFormatter.ISO_INSTANT.format(
        Instant.ofEpochSecond(responseInfo.getExpirationEpochSeconds()).atOffset(ZoneOffset.UTC).toInstant());

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

      final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      final StringWriter stringWriter = new StringWriter();
      marshaller.marshal(response, stringWriter);
      return stringWriter.toString();
    } catch (JAXBException e) {
      throw new IOException("Failed to marshal AssumeRole response", e);
    }
  }
}

