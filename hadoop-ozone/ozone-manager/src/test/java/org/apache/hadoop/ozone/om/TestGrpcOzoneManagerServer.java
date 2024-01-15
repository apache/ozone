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

package org.apache.hadoop.ozone.om;

import static org.mockito.Mockito.mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;

/**
 * Tests for GrpcOzoneManagerServer.
 */
@Timeout(30)
public class TestGrpcOzoneManagerServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestGrpcOzoneManagerServer.class);
  private OzoneManager ozoneManager;
  private OzoneManagerProtocolServerSideTranslatorPB omServerProtocol;
  private GrpcOzoneManagerServer server;

  @Test
  public void testStartStop() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ozoneManager = mock(OzoneManager.class);
    omServerProtocol = ozoneManager.getOmServerProtocol();

    server = new GrpcOzoneManagerServer(conf,
        omServerProtocol,
        ozoneManager.getDelegationTokenMgr(),
        ozoneManager.getCertificateClient(),
        "");

    try {
      server.start();
    } finally {
      server.stop();
    }
  }

}
