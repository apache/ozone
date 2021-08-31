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
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for GrpcOmTransport.
 */
public class TestGrpcOmTransport {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestGrpcOmTransport.class);
  @Rule
  public Timeout timeout = Timeout.seconds(30);


  @Test
  public void testGrpcOmTransportFactory() throws Exception {
    String omServiceId = "";
    OzoneConfiguration conf = new OzoneConfiguration();

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OmTransport omTransport = OmTransportFactory.create(conf, ugi, omServiceId);
    Assert.assertEquals(GrpcOmTransport.class.getSimpleName(),
        omTransport.getClass().getSimpleName());

  }

  @Test
  public void testStartStop() throws Exception {
    String omServiceId = "";
    OzoneConfiguration conf = new OzoneConfiguration();

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    GrpcOmTransport client = new GrpcOmTransport(conf, ugi, omServiceId);

    try {
      client.start();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      client.shutdown();
    }
  }
}
