/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.protocolPB;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;

/**
 * Tests for GrpcOmTransport.
 */
@Timeout(3000)
public class TestGrpcOmTransport {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestGrpcOmTransport.class);
  private static OzoneConfiguration conf;

  @BeforeAll
  public static void setUp() {
    conf = new OzoneConfiguration();
    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
  }

  @Test
  public void testGrpcOmTransportFactory() throws Exception {
    String omServiceId = "";
    String transportCls = GrpcOmTransportFactory.class.getName();
    conf.set(OZONE_OM_TRANSPORT_CLASS,
        transportCls);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OmTransport omTransport = OmTransportFactory.create(conf, ugi, omServiceId);
    omTransport.close();
    assertEquals(GrpcOmTransport.class.getSimpleName(),
        omTransport.getClass().getSimpleName());
  }

  @Test
  public void testHrpcOmTransportFactory() throws Exception {
    String omServiceId = "";
    String transportCls = Hadoop3OmTransportFactory.class.getName();
    conf.set(OZONE_OM_TRANSPORT_CLASS,
        transportCls);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OmTransport omTransport = OmTransportFactory.create(conf, ugi, omServiceId);
    // OmTransport should be Hadoop Rpc and
    // fail equality GrpcOmTransport equality test
    omTransport.close();
    assertNotEquals(GrpcOmTransport.class.getSimpleName(),
        omTransport.getClass().getSimpleName());
  }

  @Test
  public void testStartStop() throws Exception {
    String omServiceId = "";
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    GrpcOmTransport client = new GrpcOmTransport(conf, ugi, omServiceId);

    try {
      client.start();
    } finally {
      client.shutdown();
    }
  }
}
