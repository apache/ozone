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

package org.apache.hadoop.hdds.security;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_CIPHERS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROTOCOLS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Tests for SecurityConfig TLS protocol and cipher suite parsing.
 */
public class TestSecurityConfigTlsSettings {

  @Test
  public void testProtocolsDefault() {
    OzoneConfiguration conf = new OzoneConfiguration();
    SecurityConfig secConf = new SecurityConfig(conf);
    assertNull(secConf.getGrpcTlsProtocols());
  }

  @Test
  public void testProtocolsSingle() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_GRPC_TLS_PROTOCOLS, "TLSv1.3");
    SecurityConfig secConf = new SecurityConfig(conf);
    assertArrayEquals(new String[]{"TLSv1.3"}, secConf.getGrpcTlsProtocols());
  }

  @Test
  public void testProtocolsMultiple() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_GRPC_TLS_PROTOCOLS, "TLSv1.2,TLSv1.3");
    SecurityConfig secConf = new SecurityConfig(conf);
    assertArrayEquals(new String[]{"TLSv1.2", "TLSv1.3"}, secConf.getGrpcTlsProtocols());
  }

  @Test
  public void testProtocolsWhitespaceTrimmed() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_GRPC_TLS_PROTOCOLS, " TLSv1.3 , TLSv1.2 ");
    SecurityConfig secConf = new SecurityConfig(conf);
    assertArrayEquals(new String[]{"TLSv1.3", "TLSv1.2"}, secConf.getGrpcTlsProtocols());
  }

  @Test
  public void testProtocolsEmptyValue() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_GRPC_TLS_PROTOCOLS, "");
    SecurityConfig secConf = new SecurityConfig(conf);
    assertNull(secConf.getGrpcTlsProtocols());
  }

  @Test
  public void testCiphersDefault() {
    OzoneConfiguration conf = new OzoneConfiguration();
    SecurityConfig secConf = new SecurityConfig(conf);
    assertNull(secConf.getGrpcTlsCiphers());
  }

  @Test
  public void testCiphersMultiple() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_GRPC_TLS_CIPHERS, "TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256");
    SecurityConfig secConf = new SecurityConfig(conf);
    assertEquals(
        Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"),
        secConf.getGrpcTlsCiphers());
  }
}
