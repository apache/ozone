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
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;

/**
 * Tests the s3 EndpointBase class methods.
 * Test methods of the EndpointBase.
 */
public class TestEndpointBase {

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
}
