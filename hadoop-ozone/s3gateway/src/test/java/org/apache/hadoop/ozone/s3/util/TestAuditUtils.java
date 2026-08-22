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

package org.apache.hadoop.ozone.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AuditUtils}. */
public class TestAuditUtils {

  @Test
  public void extractsOriginalAccessKeyIdFromSessionToken() throws Exception {
    final OMTokenProto proto = OMTokenProto.newBuilder()
        .setType(OMTokenProto.Type.S3_STS_TOKEN)
        .setOriginalAccessKeyId("AKIAORIGINAL123")
        .build();

    assertEquals("AKIAORIGINAL123", AuditUtils.getStsOriginalAccessKeyId(encodeSessionToken(proto)));
  }

  @Test
  public void returnsNullWhenOriginalAccessKeyIdAbsent() throws Exception {
    final OMTokenProto proto = OMTokenProto.newBuilder()
        .setType(OMTokenProto.Type.S3_STS_TOKEN)
        .build();

    assertNull(AuditUtils.getStsOriginalAccessKeyId(encodeSessionToken(proto)));
  }

  @Test
  public void returnsNullForNullEmptyOrMalformedToken() {
    assertNull(AuditUtils.getStsOriginalAccessKeyId(null));
    assertNull(AuditUtils.getStsOriginalAccessKeyId(""));
    assertNull(AuditUtils.getStsOriginalAccessKeyId("not-a-valid-token"));
  }

  private static String encodeSessionToken(OMTokenProto proto) throws Exception {
    final Token<?> token = new Token<>(
        proto.toByteArray(), new byte[0], new Text("OzoneToken"), new Text("sts"));
    return token.encodeToUrlString();
  }
}
