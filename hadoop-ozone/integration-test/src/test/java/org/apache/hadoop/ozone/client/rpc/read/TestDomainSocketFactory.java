/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc.read;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DomainSocketFactory}'s functionality.
 */
public class TestDomainSocketFactory {

  private final InetSocketAddress localhost = InetSocketAddress.createUnresolved("localhost", 10000);

  @TempDir
  private File dir;


  private DomainSocketFactory getDomainSocketFactory() {
    // enable short-circuit read
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setShortCircuit(true);
    clientConfig.setShortCircuitReadDisableInterval(1);
    conf.setFromObject(clientConfig);
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());

    // create DomainSocketFactory
    DomainSocketFactory domainSocketFactory = DomainSocketFactory.getInstance(conf);
    assertTrue(domainSocketFactory.isServiceEnabled());
    assertTrue(domainSocketFactory.isServiceReady());
    return domainSocketFactory;
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testShortCircuitDisableTemporary() {
    DomainSocketFactory factory = getDomainSocketFactory();
    try {
      // temporary disable short-circuit read
      long pathExpireDuration = factory.getPathExpireMills();
      factory.disableShortCircuit();
      DomainSocketFactory.PathInfo pathInfo = factory.getPathInfo(localhost);
      assertEquals(DomainSocketFactory.PathState.DISABLED, pathInfo.getPathState());
      try {
        Thread.sleep(pathExpireDuration + 100);
      } catch (InterruptedException e) {
      }
      pathInfo = factory.getPathInfo(localhost);
      assertEquals(DomainSocketFactory.PathState.VALID, pathInfo.getPathState());
    } finally {
      factory.close();
    }
  }
}
