package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestSTS {

  private OzoneClient clientStub;
  private STSEndpoint stsEndpoint;
  private HttpHeaders httpHeaders;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(OZONE_S3_ADMINISTRATORS, "test-user");
    OzoneConfigurationHolder.setConfiguration(config);
    // Create client stub and object store stub
    clientStub = new OzoneClientStub();

    // Create mock HttpHeaders
    httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getHeaderString("Authorization"))
        .thenReturn("AWS4-HMAC-SHA256 Credential=test-user/20240709/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=some-signature");

    // Create STS endpoint and set client to OzoneClientStub
    stsEndpoint = EndpointBuilder.newSTSEndpointBuilder()
        .setClient(clientStub)
        .setConfig(config)
        .setHeaders(httpHeaders)
        .build();
  }

  @Test
  public void testStsAssumeRoleInSecureCluster() throws Exception {
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";

    Response response = stsEndpoint.handleSTSGet(
        "AssumeRole", roleArn, roleSessionName, 3600, "2011-06-15");

    assertEquals(200, response.getStatus());

    String responseXml = (String) response.getEntity();
    assertNotNull(responseXml);
    assertTrue(responseXml.contains("AssumeRoleResponse"));
    assertTrue(responseXml.contains("AccessKeyId"));
    assertTrue(responseXml.contains("SecretAccessKey"));
    assertTrue(responseXml.contains("SessionToken"));
    assertTrue(responseXml.contains("AssumedRoleUser"));
    assertTrue(responseXml.contains(roleArn));
  }

  // TODO: testStsWithAssumeRoleInUnsecureCluster
  // TODO: testStsUnsupportedActions
  // TODO: testInvalidLengthOfDuration
}
