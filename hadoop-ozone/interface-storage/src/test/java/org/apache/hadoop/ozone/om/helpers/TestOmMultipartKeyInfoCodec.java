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

package org.apache.hadoop.ozone.om.helpers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.Proto2CodecTestBase;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test {@link OmMultipartKeyInfo#getCodec()}.
 */
public class TestOmMultipartKeyInfoCodec
    extends Proto2CodecTestBase<OmMultipartKeyInfo> {
  @Override
  public Codec<OmMultipartKeyInfo> getCodec() {
    return OmMultipartKeyInfo.getCodec();
  }

  @Test
  public void testOmMultipartKeyInfoCodec() {
    final Codec<OmMultipartKeyInfo> codec = getCodec();
    OmMultipartKeyInfo omMultipartKeyInfo = new OmMultipartKeyInfo.Builder()
        .setUploadID(UUID.randomUUID().toString())
        .setCreationTime(Time.now())
        .setReplicationConfig(
                RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.THREE))
        .build();

    byte[] data = new byte[0];
    try {
      data = codec.toPersistedFormat(omMultipartKeyInfo);
    } catch (java.io.IOException e) {
      e.printStackTrace();
    }
    assertNotNull(data);

    OmMultipartKeyInfo multipartKeyInfo = null;
    try {
      multipartKeyInfo = codec.fromPersistedFormat(data);
    } catch (java.io.IOException e) {
      e.printStackTrace();
    }
    assertEquals(omMultipartKeyInfo, multipartKeyInfo);

    // When random byte data passed returns null.
    try {
      codec.fromPersistedFormat("random".getBytes(UTF_8));
    } catch (IllegalArgumentException ex) {
      assertThat(ex).hasMessage("Can't encode the the raw data from the byte array");
    } catch (java.io.IOException e) {
      e.printStackTrace();
    }
  }
}
