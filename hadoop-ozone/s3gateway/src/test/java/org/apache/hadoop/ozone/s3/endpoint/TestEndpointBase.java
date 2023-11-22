/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * Tests the s3 EndpointBase class methods.
 */
package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
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
      @Override
      public void init() { }
    };

    Map<String, String> filteredCustomMetadata =
            endpointBase.getCustomMetadataFromHeaders(s3requestHeaders);
    assertTrue(filteredCustomMetadata.containsKey("custom-key1"));
    assertEquals(
            "custom-value1", filteredCustomMetadata.get("custom-key1"));
    assertTrue(filteredCustomMetadata.containsKey("custom-key2"));
    assertEquals(
            "custom-value2", filteredCustomMetadata.get("custom-key2"));
    assertFalse(
            filteredCustomMetadata.containsKey(OzoneConsts.GDPR_FLAG));
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
      @Override
      public void init() { }
    };

    OS3Exception e = assertThrows(OS3Exception.class, () -> endpointBase
        .getCustomMetadataFromHeaders(s3requestHeaders),
        "getCustomMetadataFromHeaders should fail." +
            " Expected OS3Exception not thrown");
    assertTrue(e.getCode().contains("MetadataTooLarge"));
  }

}
