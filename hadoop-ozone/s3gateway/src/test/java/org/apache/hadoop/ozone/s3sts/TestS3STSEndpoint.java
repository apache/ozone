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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.AssumeRoleResponseInfo;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.apache.hadoop.ozone.s3.exception.OSTSException;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * Test for S3 STS endpoint.
 */
public class TestS3STSEndpoint {
  private S3STSEndpoint endpoint;
  private ObjectStore objectStore;
  private AuditLogger auditLogger;
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String ROLE_SESSION_NAME = "test-session";
  private static final String ROLE_USER_ARN = "arn:aws:sts::123456789012:assumed-role/test-role/" + ROLE_SESSION_NAME;
  private static final String STS_NS = "https://sts.amazonaws.com/doc/2011-06-15/";
  private static final String AWS_FAULT_NS = "http://webservices.amazon.com/AWSFault/2005-15-09";

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3_ADMINISTRATORS, "test-user");
    OzoneConfigurationHolder.setConfiguration(config);
    OzoneClient clientStub = spy(new OzoneClientStub());

    final ContainerRequestContext context = mock(ContainerRequestContext.class);
    final RequestIdentifier requestIdentifier = mock(RequestIdentifier.class);
    final UriInfo uriInfo = mock(UriInfo.class);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());

    // Stub assumeRole to return deterministic credentials.
    objectStore = mock(ObjectStore.class);
    when(objectStore.assumeRole(anyString(), anyString(), anyInt(), any(), anyString()))
        .thenReturn(new AssumeRoleResponseInfo(
            "ASIA1234567890123456",
            "mySecretAccessKey",
            "session-token",
            Instant.now().plusSeconds(3600).getEpochSecond(),
            "AROA1234567890123456:test-session"));
    when(clientStub.getObjectStore()).thenReturn(objectStore);

    endpoint = new S3STSEndpoint();
    endpoint.setClient(clientStub);
    endpoint.setContext(context);
    auditLogger = mock(AuditLogger.class);
    endpoint.setAuditLogger(auditLogger);
    
    when(requestIdentifier.getRequestId()).thenReturn("test-request-id");
    endpoint.setRequestIdentifier(requestIdentifier);

    SignatureInfo signatureInfo = new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setAwsAccessId("test-user")
        .setSignature("some-signature")
        .setStringToSign("dummy-string")
        .build();
    endpoint.setSignatureInfo(signatureInfo);
  }

  @Test
  public void testStsAssumeRoleValidForGetMethod() throws Exception {
    final Response response = endpoint.get("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null);

    assertEquals(200, response.getStatus());
    verify(auditLogger).logWriteSuccess(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteFailure(any(AuditMessage.class));

    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);

    // Parse response XML and verify values
    final Document doc = parseXml(responseXml);

    final Element root = doc.getDocumentElement();
    assertEquals("AssumeRoleResponse", root.getLocalName());
    assertEquals(STS_NS, root.getNamespaceURI());
    // Ensure the response uses the default namespace (no prefix like "ns2:")
    assertEquals("AssumeRoleResponse", root.getNodeName());

    // Verify some key elements are present in the STS namespace
    assertNotNull(doc.getElementsByTagNameNS(STS_NS, "AssumeRoleResult").item(0));
    assertNotNull(doc.getElementsByTagNameNS(STS_NS, "Credentials").item(0));
    assertNotNull(doc.getElementsByTagNameNS(STS_NS, "AccessKeyId").item(0));

    final String accessKeyId = doc.getElementsByTagName("AccessKeyId").item(0).getTextContent();
    assertEquals("ASIA1234567890123456", accessKeyId);

    final String secretAccessKey = doc.getElementsByTagName("SecretAccessKey").item(0).getTextContent();
    assertEquals("mySecretAccessKey", secretAccessKey);

    final String sessionToken = doc.getElementsByTagName("SessionToken").item(0).getTextContent();
    assertEquals("session-token", sessionToken);

    final String arn = doc.getElementsByTagName("Arn").item(0).getTextContent();
    assertEquals(ROLE_USER_ARN, arn);
  }

  @Test
  public void testStsAssumeRoleValidForPostMethod() throws Exception {
    //noinspection resource
    final Response response = endpoint.post("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null);

    assertEquals(200, response.getStatus());
    verify(auditLogger).logWriteSuccess(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteFailure(any(AuditMessage.class));
    final String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);

    final Document doc = parseXml(responseXml);
    final Element root = doc.getDocumentElement();
    assertEquals("AssumeRoleResponse", root.getLocalName());
    assertEquals(STS_NS, root.getNamespaceURI());
    // Ensure the response uses the default namespace (no prefix like "ns2:")
    assertEquals("AssumeRoleResponse", root.getNodeName());

    // Verify some key elements are present in the STS namespace
    assertNotNull(doc.getElementsByTagNameNS(STS_NS, "AssumeRoleResult").item(0));
    assertNotNull(doc.getElementsByTagNameNS(STS_NS, "Credentials").item(0));
    assertNotNull(doc.getElementsByTagNameNS(STS_NS, "AccessKeyId").item(0));

    final String accessKeyId = doc.getElementsByTagName("AccessKeyId").item(0).getTextContent();
    assertEquals("ASIA1234567890123456", accessKeyId);

    final String secretAccessKey = doc.getElementsByTagName("SecretAccessKey").item(0).getTextContent();
    assertEquals("mySecretAccessKey", secretAccessKey);

    final String sessionToken = doc.getElementsByTagName("SessionToken").item(0).getTextContent();
    assertEquals("session-token", sessionToken);

    final String arn = doc.getElementsByTagName("Arn").item(0).getTextContent();
    assertEquals(ROLE_USER_ARN, arn);
  }

  @Test
  public void testStsNullAction() throws Exception {
    final Response response = endpoint.get(null, ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null);

    assertEquals(400, response.getStatus());
    verifyNoInteractions(auditLogger);
    final String errorMessage = (String) response.getEntity();
    assertEquals("<UnknownOperationException/>", errorMessage);

    final Document doc = parseXml(errorMessage);
    final Element root = doc.getDocumentElement();
    assertEquals("UnknownOperationException", root.getLocalName());
  }

  @Test
  public void testStsUnsupportedActionWithVersionSupplied() throws Exception {
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("UnsupportedAction", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verifyNoInteractions(auditLogger);

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), AWS_FAULT_NS, "Sender", "InvalidAction",
        "Could not find operation UnsupportedAction for version 2011-06-15");
  }

  @Test
  public void testStsUnsupportedActionWithVersionNotSupplied() throws Exception {
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("UnsupportedAction", ROLE_ARN, ROLE_SESSION_NAME, 3600, null, null));

    assertEquals(400, ex.getHttpCode());
    verifyNoInteractions(auditLogger);

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), AWS_FAULT_NS, "Sender", "InvalidAction",
        "Could not find operation UnsupportedAction for version NO_VERSION_SPECIFIED");
  }

  @Test
  public void testStsAssumeRoleWithInvalidVersion() throws Exception {
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2000-01-01", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), AWS_FAULT_NS, "Sender", "InvalidAction",
        "Could not find operation AssumeRole for version 2000-01-01");
  }

  @Test
  public void testStsInvalidDuration() throws Exception {
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, -1, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError", "Invalid Value: DurationSeconds");
  }

  @Test
  public void testStsNullDurationUsesDefault3600() throws Exception {
    final Response response = endpoint.get(
        "AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, null, "2011-06-15", null);
    assertEquals(200, response.getStatus());
    verify(auditLogger).logWriteSuccess(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteFailure(any(AuditMessage.class));

    final ArgumentCaptor<Integer> durationCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(objectStore).assumeRole(anyString(), anyString(), durationCaptor.capture(), any(), anyString());
    assertEquals(3600, durationCaptor.getValue());
  }

  @Test
  public void testStsPolicyTooLarge() throws Exception {
    final String tooLargePolicy = RandomStringUtils.insecure().nextAlphanumeric(2049);

    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", tooLargePolicy));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError",
        "Value '" + tooLargePolicy + "' at 'policy' failed to satisfy constraint: Member " +
        "must have length less than or equal to 2048");
  }

  @Test
  public void testStsInvalidRoleArn() throws Exception {
    final String invalidRoleArn = "arn:awsNotValid::123456789012:role/test-role";
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", invalidRoleArn, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError",
        "Invalid role ARN (does not start with arn:aws:iam::)");
  }

  @Test
  public void testStsMissingRoleArn() throws Exception {
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", null, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError", "Value null at 'roleArn'");
  }

  @Test
  public void testStsInvalidRoleArnMissingRoleName() throws Exception {
    final String invalidRoleArn = "arn:aws:iam::123456789012:role/";
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", invalidRoleArn, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    assertEquals("ValidationError", ex.getCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError", "Invalid role ARN: missing role name");
  }

  @Test
  public void testStsInvalidRoleArnMissingAccountId() throws Exception {
    final String invalidRoleArn = "arn:aws:iam:::role/test-role";
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", invalidRoleArn, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    assertEquals("ValidationError", ex.getCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError", "Invalid AWS account ID in ARN"
    );
  }

  @Test
  public void testStsWhenActionNotImplemented() throws Exception {
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("GetSessionToken", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(501, ex.getHttpCode());
    verifyNoInteractions(auditLogger);

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), AWS_FAULT_NS, "Sender", "InvalidAction",
        "Operation GetSessionToken is not supported yet.");
  }

  @Test
  public void testStsMissingRoleSessionName() throws Exception {
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, null, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError", "Value null at 'roleSessionName'");
  }

  @Test
  public void testStsInvalidRoleSessionNameWithInvalidCharacter() throws Exception {
    final String invalidSession = "test/session";
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, invalidSession, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(
        ex.toXml(), STS_NS, "Sender", "ValidationError", "1 validation error detected: " +
        "Invalid character '/' in RoleSessionName: it must be 2-64 characters long and contain only alphanumeric " +
        "characters and +, =, ,, ., @, -");
  }

  @Test
  public void testStsInvalidRoleSessionNameTooShort() throws Exception {
    final String invalidSession = "a";
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, invalidSession, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(
        ex.toXml(), STS_NS, "Sender", "ValidationError", "1 validation error detected: Invalid RoleSessionName " +
        "length 1: it must be 2-64 characters long and contain only alphanumeric characters and +, =, ,, ., @, -");
  }

  @Test
  public void testStsInvalidRoleArnResourceType() throws Exception {
    // Resource type must be role, not user
    final String invalidRoleArn = "arn:aws:iam::123456789012:user/test-user";
    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", invalidRoleArn, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "ValidationError", "Invalid role ARN (unexpected field count)");
  }

  @Test
  public void testStsInternalFailureWhenBackendThrows() throws Exception {
    when(objectStore.assumeRole(anyString(), anyString(), anyInt(), any(), anyString()))
        .thenThrow(new RuntimeException("some unexpected error"));

    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(500, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Receiver", "InternalFailure", "An internal error has occurred.");
  }

  @Test
  public void testStsAccessDenied() throws Exception {
    when(objectStore.assumeRole(anyString(), anyString(), anyInt(), any(), anyString()))
        .thenThrow(new OMException("Permission denied", OMException.ResultCodes.ACCESS_DENIED));

    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(403, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Sender", "AccessDenied",
        "User is not authorized to perform: sts:AssumeRole on resource: " + ROLE_ARN);
  }

  @Test
  public void testStsIOExceptionWrappedAsInternalFailure() throws Exception {
    when(objectStore.assumeRole(anyString(), anyString(), anyInt(), any(), anyString()))
        .thenThrow(new IOException("An IO error occurred"));

    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, ROLE_SESSION_NAME, 3600, "2011-06-15", null));

    assertEquals(500, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);
    assertStsErrorXml(ex.toXml(), STS_NS, "Receiver", "InternalFailure", "An internal error has occurred.");
  }

  @Test
  public void testStsMultipleValidationErrors() throws Exception {
    final String invalidRoleSessionName = "test/session";
    final String tooLargePolicy = RandomStringUtils.insecure().nextAlphanumeric(2049);
    final int invalidDurationSeconds = -1;

    final OSTSException ex = assertThrows(OSTSException.class, () ->
        endpoint.get("AssumeRole", ROLE_ARN, invalidRoleSessionName, invalidDurationSeconds, "2011-06-15",
            tooLargePolicy));

    assertEquals(400, ex.getHttpCode());
    verify(auditLogger).logWriteFailure(any(AuditMessage.class));
    verify(auditLogger, never()).logWriteSuccess(any(AuditMessage.class));

    final String requestId = "test-request-id";
    ex.setRequestId(requestId);

    final String xml = ex.toXml();
    // The order of individual validation errors is not guaranteed because it's a HashSet, so check
    // that multiple messages are included
    final Document doc = parseXml(xml);
    final String message = doc.getElementsByTagName("Message").item(0).getTextContent();
    assertTrue(message.contains("3 validation errors detected"));
    assertTrue(message.contains("Invalid Value: DurationSeconds"));
    assertTrue(message.contains("Invalid character '/' in RoleSessionName"));
    assertTrue(message.contains(
        "'policy' failed to satisfy constraint: Member must have length less than or equal to 2048"));
  }

  private static Document parseXml(String xml) throws Exception {
    final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    return documentBuilder.parse(new InputSource(new StringReader(xml)));
  }

  private static void assertStsErrorXml(String xml, String expectedNamespace, String expectedType, String expectedCode,
      String expectedMessageContains) throws Exception {
    final Document doc = parseXml(xml);
    final Element root = doc.getDocumentElement();
    assertEquals("ErrorResponse", root.getLocalName());
    assertEquals(expectedNamespace, root.getNamespaceURI());

    final String type = doc.getElementsByTagName("Type").item(0).getTextContent();
    assertEquals(expectedType, type);

    final String code = doc.getElementsByTagName("Code").item(0).getTextContent();
    assertEquals(expectedCode, code);

    final String message = doc.getElementsByTagName("Message").item(0).getTextContent();
    assertNotNull(message);
    assertTrue(message.contains(expectedMessageContains), "Expected message to contain: " + expectedMessageContains);

    final String requestId = doc.getElementsByTagName("RequestId").item(0).getTextContent();
    assertEquals("test-request-id", requestId);
  }
}
