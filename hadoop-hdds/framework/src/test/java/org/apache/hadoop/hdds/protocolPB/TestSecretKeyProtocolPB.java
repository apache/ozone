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

package org.apache.hadoop.hdds.protocolPB;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolDatanode;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolOm;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the Hadoop RPC metadata for the role-specific secret key protocols.
 */
public class TestSecretKeyProtocolPB {

  private final Configuration conf = new Configuration(false);

  @BeforeEach
  public void setup() {
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, "dn/_HOST@EXAMPLE.COM");
    conf.set("ozone.om.kerberos.principal", "om/_HOST@EXAMPLE.COM");
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, "scm/_HOST@EXAMPLE.COM");
  }

  @Test
  public void testDatanodeProtocolMetadata() {
    String protocolName = "org.apache.hadoop.hdds.protocol.SecretKeyProtocolDatanode";
    assertEquals(protocolName, SecretKeyProtocolDatanode.class.getName());
    assertEquals(protocolName, RPC.getProtocolName(SecretKeyProtocolDatanodePB.class));
    assertEquals(1L, RPC.getProtocolVersion(SecretKeyProtocolDatanodePB.class));

    KerberosInfo kerberosInfo = SecretKeyProtocolDatanodePB.class.getAnnotation(KerberosInfo.class);
    assertNotNull(kerberosInfo);
    assertEquals(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, kerberosInfo.serverPrincipal());
    assertEquals(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, kerberosInfo.clientPrincipal());

    // Service authorization resolves the client principal using the Java protocol identity.
    assertEquals("dn/_HOST@EXAMPLE.COM", SecurityUtil.getClientPrincipal(SecretKeyProtocolDatanode.class, conf));
  }

  @Test
  public void testOmProtocolMetadata() {
    String protocolName = "org.apache.hadoop.hdds.protocol.SecretKeyProtocolOm";
    assertEquals(protocolName, SecretKeyProtocolOm.class.getName());
    assertEquals(protocolName, RPC.getProtocolName(SecretKeyProtocolOmPB.class));
    assertEquals(1L, RPC.getProtocolVersion(SecretKeyProtocolOmPB.class));

    KerberosInfo kerberosInfo = SecretKeyProtocolOmPB.class.getAnnotation(KerberosInfo.class);
    assertNotNull(kerberosInfo);
    assertEquals(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, kerberosInfo.serverPrincipal());
    assertEquals("ozone.om.kerberos.principal", kerberosInfo.clientPrincipal());

    assertEquals("om/_HOST@EXAMPLE.COM", SecurityUtil.getClientPrincipal(SecretKeyProtocolOm.class, conf));
  }

  @Test
  public void testScmProtocolMetadata() {
    String protocolName = "org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm";
    assertEquals(protocolName, SecretKeyProtocolScm.class.getName());
    assertEquals(protocolName, RPC.getProtocolName(SecretKeyProtocolScmPB.class));
    assertEquals(1L, RPC.getProtocolVersion(SecretKeyProtocolScmPB.class));

    KerberosInfo kerberosInfo = SecretKeyProtocolScmPB.class.getAnnotation(KerberosInfo.class);
    assertNotNull(kerberosInfo);
    assertEquals(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, kerberosInfo.serverPrincipal());
    assertEquals(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, kerberosInfo.clientPrincipal());

    assertEquals("scm/_HOST@EXAMPLE.COM", SecurityUtil.getClientPrincipal(SecretKeyProtocolScm.class, conf));
  }
}
