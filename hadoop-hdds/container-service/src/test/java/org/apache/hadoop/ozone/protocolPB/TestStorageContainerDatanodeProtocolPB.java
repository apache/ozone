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

package org.apache.hadoop.ozone.protocolPB;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.protocol.ReconDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.jupiter.api.Test;

/**
 * Tests the Hadoop RPC metadata for the SCM datanode protocol.
 */
public class TestStorageContainerDatanodeProtocolPB {

  private static final String PROTOCOL_NAME = "org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol";

  @Test
  public void testProtocolMetadata() {
    assertEquals(PROTOCOL_NAME, StorageContainerDatanodeProtocol.class.getName());
    assertEquals(PROTOCOL_NAME, RPC.getProtocolName(StorageContainerDatanodeProtocolPB.class));
    assertEquals(1L, RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class));

    KerberosInfo kerberosInfo = StorageContainerDatanodeProtocolPB.class.getAnnotation(KerberosInfo.class);
    assertNotNull(kerberosInfo);
    assertEquals(ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY, kerberosInfo.serverPrincipal());
    assertEquals(HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, kerberosInfo.clientPrincipal());
  }

  @Test
  public void testServiceAuthorizationClientPrincipal() {
    Configuration conf = new Configuration(false);
    conf.set(HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, "dn/_HOST@EXAMPLE.COM");
    // Service authorization reads the class named by @ProtocolInfo, not the PB class
    assertEquals("dn/_HOST@EXAMPLE.COM", SecurityUtil.getClientPrincipal(StorageContainerDatanodeProtocol.class, conf));
    assertEquals(ReconDatanodeProtocol.class.getName(), RPC.getProtocolName(ReconDatanodeProtocolPB.class));
    assertEquals("dn/_HOST@EXAMPLE.COM", SecurityUtil.getClientPrincipal(ReconDatanodeProtocol.class, conf));
  }
}
