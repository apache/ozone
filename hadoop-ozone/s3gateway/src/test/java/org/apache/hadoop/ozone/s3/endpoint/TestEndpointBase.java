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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RESERVED_USER_METADATA_KEY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

/**
 * Tests the s3 EndpointBase class methods.
 * Test methods of the EndpointBase.
 */
public class TestEndpointBase {
  private static final String ORIGINAL_ACCESS_KEY_ID_PARAM = "originalAccessKeyId";
  private static final String FORGED_STS_ORIGINAL_ACCESS_KEY_ID = "FORGED-ORIGINAL-ACCESS-KEY";
  private static final String STS_TEMP_ACCESS_KEY_ID = "ASIAEXAMPLE123";

  /**
   * Verify s3 metadata key "gdprEnabled" can't be set up directly
   * from the normal client's request,
   * it should be decided on the server side.
   */
  @Test
  public void testFilterGDPRFromCustomMetadataHeaders()
          throws OS3Exception {
    MultivaluedMap<String, String> s3requestHeaders
            = new MultivaluedHashMap<>();
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key1", "custom-value1");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key2", "custom-value2");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + OzoneConsts.GDPR_FLAG, "true");

    EndpointBase endpointBase = new EndpointBase() {
    };

    Map<String, String> filteredCustomMetadata =
            endpointBase.getCustomMetadataFromHeaders(s3requestHeaders);
    assertThat(filteredCustomMetadata).containsKey("custom-key1");
    assertEquals(
            "custom-value1", filteredCustomMetadata.get("custom-key1"));
    assertThat(filteredCustomMetadata).containsKey("custom-key2");
    assertEquals(
            "custom-value2", filteredCustomMetadata.get("custom-key2"));
    assertThat(filteredCustomMetadata).doesNotContainKey(OzoneConsts.GDPR_FLAG);
  }

  /**
   * Verify s3 request metadata size should be smaller than 2 KB.
   */
  @Test
  public void testCustomMetadataHeadersSizeOverbig() {
    MultivaluedMap<String, String> s3requestHeaders
            = new MultivaluedHashMap<>();
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key1", "custom-value1");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key2", "custom-value2");
    s3requestHeaders.add(
            CUSTOM_METADATA_HEADER_PREFIX + "custom-key3",
            new String(new byte[3000], StandardCharsets.UTF_8));

    EndpointBase endpointBase = new EndpointBase() {
    };

    OS3Exception e = assertThrows(OS3Exception.class, () -> endpointBase
        .getCustomMetadataFromHeaders(s3requestHeaders),
        "getCustomMetadataFromHeaders should fail." +
            " Expected OS3Exception not thrown");
    assertThat(e.getCode()).contains("MetadataTooLarge");
  }

  @Test
  public void testCustomMetadataHeadersWithUpperCaseHeaders() throws OS3Exception {
    MultivaluedMap<String, String> s3requestHeaders = new MultivaluedHashMap<>();
    String key = "CUSTOM-KEY";
    String value = "custom-value1";
    s3requestHeaders.add(CUSTOM_METADATA_HEADER_PREFIX.toUpperCase(Locale.ROOT) + key, value);

    EndpointBase endpointBase = new EndpointBase() {
    };

    Map<String, String> customMetadata = endpointBase.getCustomMetadataFromHeaders(s3requestHeaders);

    assertEquals(value, customMetadata.get(key));
  }

  @Test
  public void testAccessDeniedResultCodes() {
    final EndpointBase endpointBase = new EndpointBase() {
      @Override
      public void init() { }
    };

    assertTrue(endpointBase.isAccessDenied(new OMException(ResultCodes.PERMISSION_DENIED)));
    assertTrue(endpointBase.isAccessDenied(new OMException(ResultCodes.INVALID_TOKEN)));
    assertTrue(endpointBase.isAccessDenied(new OMException(ResultCodes.REVOKED_TOKEN)));
    assertFalse(endpointBase.isAccessDenied(new OMException(ResultCodes.INTERNAL_ERROR)));
    assertFalse(endpointBase.isAccessDenied(new OMException(ResultCodes.BUCKET_NOT_FOUND)));
  }

  @Test
  public void testExpiredTokenResultCode() {
    final EndpointBase endpointBase = new EndpointBase() {
      @Override
      public void init() { }
    };

    assertTrue(endpointBase.isExpiredToken(new OMException(ResultCodes.TOKEN_EXPIRED)));
    assertFalse(endpointBase.isExpiredToken(new OMException(ResultCodes.INVALID_TOKEN)));
  }

  @Test
  public void testAuditMessageIncludesValidatedStsOriginalAccessKeyId() throws Exception {
    final String originalAccessKeyId = "AKIAORIGINAL123";
    // Pass the forged token to newAuditEndpoint because we need a session token present (so the request is
    // for STS), but deliberately make its embedded originalAccessKeyId wrong, so the test can prove the audit path
    // ignores it and only trusts the validated field.
    final AuditEndpoint endpointBase = newAuditEndpoint(stsSignatureInfoWithForgedOriginalAccessKeyId());
    endpointBase.setValidatedStsOriginalAccessKeyIdForTest();

    assertThat(endpointBase.auditMessageForTest().getParams())
        .containsEntry(ORIGINAL_ACCESS_KEY_ID_PARAM, originalAccessKeyId)
        .doesNotContainValue(FORGED_STS_ORIGINAL_ACCESS_KEY_ID);
  }

  @Test
  public void testAuditMessageResolvesValidatedStsOriginalAccessKeyIdFromOm() throws Exception {
    final String originalAccessKeyId = "AKIAORIGINAL123";
    final StsAuditEndpointFixture fixture = newStsAuditEndpointFixture(
        stsSignatureInfoWithForgedOriginalAccessKeyId(),
        (objectStore, s3AuthRef) -> stubGetS3VolumeContext(
            objectStore, invocation -> {
            final S3Auth auth = s3AuthRef.get();
            if (auth != null) {
              auth.setValidatedStsOriginalAccessKeyId(originalAccessKeyId);
            }
            final OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
                .setVolume("s3v")
                .setAdminName("admin")
                .setOwnerName("owner")
                .build();
            return S3VolumeContext.newBuilder()
                .setOmVolumeArgs(volumeArgs)
                .setUserPrincipal("alice")
                .setStsOriginalAccessKeyId(originalAccessKeyId)
                .build();
            }));

    assertThat(fixture.getEndpoint().auditMessageForTest().getParams())
        .containsEntry(ORIGINAL_ACCESS_KEY_ID_PARAM, originalAccessKeyId)
        .doesNotContainValue(FORGED_STS_ORIGINAL_ACCESS_KEY_ID);
    verify(fixture.getObjectStore()).getS3VolumeContext();
  }

  @Test
  public void testAuditMessageOmitsStsOriginalAccessKeyIdWhenNotValidated() throws Exception {
    final AuditEndpoint endpointBase = newAuditEndpoint(stsSignatureInfoWithForgedOriginalAccessKeyId());

    assertThat(endpointBase.auditMessageForTest().getParams())
        .doesNotContainKey(ORIGINAL_ACCESS_KEY_ID_PARAM);
  }

  @Test
  public void testFailureAuditOmitsStsOriginalAccessKeyIdWhenNotValidated() throws Exception {
    final StsAuditEndpointFixture fixture = newStsAuditEndpointFixture(stsSignatureInfoWithForgedOriginalAccessKeyId());

    assertThat(fixture.getEndpoint().auditMessageForFailureTest(
        new OMException("STS token validation failed", ResultCodes.INVALID_TOKEN)).getParams())
        .doesNotContainKey(ORIGINAL_ACCESS_KEY_ID_PARAM)
        .doesNotContainValue(FORGED_STS_ORIGINAL_ACCESS_KEY_ID);
    verify(fixture.getObjectStore(), never()).getS3VolumeContext();
  }

  @Test
  public void testAuditMessageSuccessIgnoresRuntimeExceptionFromOmResolution() throws Exception {
    final StsAuditEndpointFixture fixture = newStsAuditEndpointFixture(
        stsSignatureInfoWithForgedOriginalAccessKeyId(), objectStore -> stubGetS3VolumeContextToThrow(
            objectStore, new RuntimeException("OM unavailable")));

    assertThat(fixture.getEndpoint().auditMessageForTest().getParams())
        .doesNotContainKey(ORIGINAL_ACCESS_KEY_ID_PARAM)
        .doesNotContainValue(FORGED_STS_ORIGINAL_ACCESS_KEY_ID);
    verify(fixture.getObjectStore()).getS3VolumeContext();
  }

  @Test
  public void testAuditMessageOmitsStsOriginalAccessKeyIdForNonStsRequest() {
    final SignatureInfo signatureInfo = new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setAwsAccessId("AKIAEXAMPLE123")
        .setSignature("signature")
        .setStringToSign("string-to-sign")
        .build();
    final AuditEndpoint endpointBase = newAuditEndpoint(signatureInfo);

    assertThat(endpointBase.auditMessageForTest().getParams())
        .doesNotContainKey(ORIGINAL_ACCESS_KEY_ID_PARAM);
  }

  @Test
  public void testListS3BucketsHandlesRuntimeExceptionWrappingOMException() throws Exception {
    final EndpointBase endpointBase = new EndpointBase() {
      @Override
      public void init() { }

      @Override
      protected OzoneVolume getVolume() {
        final OzoneVolume volume = mock(OzoneVolume.class);
        when(volume.listBuckets(anyString())).thenThrow(
            new RuntimeException(new OMException("Permission Denied", ResultCodes.PERMISSION_DENIED)));
        return volume;
      }
    };

    final OS3Exception e = assertThrows(
        OS3Exception.class, () -> endpointBase.listS3Buckets(
            "prefix", volume -> { }), "listS3Buckets should fail.");

    // Ensure we get the correct code
    assertEquals("AccessDenied", e.getCode());
  }

  @Test
  public void testListS3BucketsHandlesRuntimeExceptionWrappingOMExceptionVolumeNotFound() throws Exception {
    final EndpointBase endpointBase = new EndpointBase() {
      @Override
      public void init() { }

      @Override
      protected OzoneVolume getVolume() {
        final OzoneVolume volume = mock(OzoneVolume.class);
        when(volume.listBuckets(anyString())).thenThrow(
            new RuntimeException(new OMException("Volume Not Found", ResultCodes.VOLUME_NOT_FOUND)));
        return volume;
      }
    };

    // Ensure we get an empty iterator
    assertFalse(endpointBase.listS3Buckets("prefix", volume -> { }).hasNext());
  }

  @ParameterizedTest
  @MethodSource("reservedInternalMetadataKeyPrefixCases")
  public void testRejectReservedInternalMetadataKeyPrefix(String metadataKey) {
    MultivaluedMap<String, String> s3requestHeaders = new MultivaluedHashMap<>();
    s3requestHeaders.add(CUSTOM_METADATA_HEADER_PREFIX + metadataKey, "user-value");

    EndpointBase endpointBase = new EndpointBase() {
    };

    OS3Exception e = assertThrows(OS3Exception.class, () -> endpointBase
        .getCustomMetadataFromHeaders(s3requestHeaders));
    assertThat(e.getCode()).contains(INVALID_ARGUMENT.getCode());
    assertThat(e.getErrorMessage()).contains(RESERVED_USER_METADATA_KEY_PREFIX);
  }

  private static Stream<String> reservedInternalMetadataKeyPrefixCases() {
    return Stream.of(
        RESERVED_USER_METADATA_KEY_PREFIX + "cache-control",
        RESERVED_USER_METADATA_KEY_PREFIX.toUpperCase(Locale.ROOT) + "cache-control");
  }

  private static String encodeSessionToken(OMTokenProto proto) throws Exception {
    final Token<?> token = new Token<>(
        proto.toByteArray(), new byte[0], new Text("OzoneToken"), new Text("sts"));
    return token.encodeToUrlString();
  }

  private static SignatureInfo stsSignatureInfoWithForgedOriginalAccessKeyId() throws Exception {
    final OMTokenProto proto = OMTokenProto.newBuilder()
        .setType(OMTokenProto.Type.S3_STS_TOKEN)
        .setOriginalAccessKeyId(FORGED_STS_ORIGINAL_ACCESS_KEY_ID)
        .build();
    return new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setAwsAccessId(STS_TEMP_ACCESS_KEY_ID)
        .setSignature("signature")
        .setStringToSign("string-to-sign")
        .setSessionToken(encodeSessionToken(proto))
        .build();
  }

  private static void stubGetS3VolumeContext(ObjectStore objectStore, Answer<S3VolumeContext> answer) {
    try {
      doAnswer(answer).when(objectStore).getS3VolumeContext();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void stubGetS3VolumeContextToThrow(ObjectStore objectStore, RuntimeException toThrow) {
    try {
      doThrow(toThrow).when(objectStore).getS3VolumeContext();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static StsAuditEndpointFixture newStsAuditEndpointFixture(SignatureInfo signatureInfo)
      throws Exception {
    return newStsAuditEndpointFixture(signatureInfo, (Consumer<ObjectStore>) objectStore -> { });
  }

  private static StsAuditEndpointFixture newStsAuditEndpointFixture(
      SignatureInfo signatureInfo,
      Consumer<ObjectStore> objectStoreConfigurer) throws Exception {
    return newStsAuditEndpointFixture(signatureInfo, (objectStore, s3AuthRef) ->
        objectStoreConfigurer.accept(objectStore));
  }

  private static StsAuditEndpointFixture newStsAuditEndpointFixture(
      SignatureInfo signatureInfo,
      BiConsumer<ObjectStore, AtomicReference<S3Auth>> objectStoreConfigurer) throws Exception {
    final OzoneClient client = mock(OzoneClient.class);
    final ObjectStore objectStore = mock(ObjectStore.class);
    final ClientProtocol clientProtocol = mock(ClientProtocol.class);
    final AtomicReference<S3Auth> s3AuthRef = new AtomicReference<>();

    doAnswer(invocation -> {
      s3AuthRef.set(invocation.getArgument(0));
      return null;
    }).when(clientProtocol).setThreadLocalS3Auth(any(S3Auth.class));
    when(clientProtocol.getThreadLocalS3Auth()).thenAnswer(invocation -> s3AuthRef.get());
    when(client.getObjectStore()).thenReturn(objectStore);
    when(objectStore.getClientProxy()).thenReturn(clientProtocol);
    objectStoreConfigurer.accept(objectStore, s3AuthRef);

    final AuditEndpoint endpoint = new EndpointBuilder<>(AuditEndpoint::new)
        .setClient(client)
        .setSignatureInfo(signatureInfo)
        .build();
    return new StsAuditEndpointFixture(endpoint, objectStore);
  }

  private static AuditEndpoint newAuditEndpoint(SignatureInfo signatureInfo) {
    return new EndpointBuilder<>(AuditEndpoint::new)
        .setSignatureInfo(signatureInfo)
        .build();
  }

  private static final class StsAuditEndpointFixture {
    private final AuditEndpoint endpoint;
    private final ObjectStore objectStore;

    private StsAuditEndpointFixture(AuditEndpoint endpoint, ObjectStore objectStore) {
      this.endpoint = endpoint;
      this.objectStore = objectStore;
    }

    private AuditEndpoint getEndpoint() {
      return endpoint;
    }

    private ObjectStore getObjectStore() {
      return objectStore;
    }
  }

  private static final class AuditEndpoint extends EndpointBase {
    private AuditMessage.Builder auditMessageForTest() {
      return auditMessageForSuccess(S3GAction.GET_KEY);
    }

    private AuditMessage.Builder auditMessageForFailureTest(Throwable throwable) {
      return auditMessageForFailure(S3GAction.GET_KEY, throwable);
    }
  }

}
