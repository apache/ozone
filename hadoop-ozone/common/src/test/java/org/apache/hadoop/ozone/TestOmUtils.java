/**
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

package org.apache.hadoop.ozone;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Unit tests for {@link OmUtils}.
 */
public class TestOmUtils {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(60_000);


  @Test
  public void createOMDirCreatesDirectoryIfNecessary() throws IOException {
    File parent = folder.newFolder();
    File omDir = new File(new File(parent, "sub"), "dir");
    assertFalse(omDir.exists());

    OmUtils.createOMDir(omDir.getAbsolutePath());

    assertTrue(omDir.exists());
  }

  @Test
  public void createOMDirDoesNotThrowIfAlreadyExists() throws IOException {
    File omDir = folder.newFolder();
    assertTrue(omDir.exists());

    OmUtils.createOMDir(omDir.getAbsolutePath());

    assertTrue(omDir.exists());
  }

  @Test(expected = IllegalArgumentException.class)
  public void createOMDirThrowsIfCannotCreate() throws IOException {
    File parent = folder.newFolder();
    File omDir = new File(new File(parent, "sub"), "dir");
    assumeTrue(parent.setWritable(false, false));

    OmUtils.createOMDir(omDir.getAbsolutePath());

    // expecting exception
  }

  @Test
  public void testGetOmHAAddressesById() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SERVICE_IDS_KEY, "ozone1");
    conf.set("ozone.om.nodes.ozone1", "node1,node2,node3");
    conf.set("ozone.om.address.ozone1.node1", "1.1.1.1");
    conf.set("ozone.om.address.ozone1.node2", "1.1.1.2");
    conf.set("ozone.om.address.ozone1.node3", "1.1.1.3");
    Map<String, List<InetSocketAddress>> addresses =
        OmUtils.getOmHAAddressesById(conf);
    assertFalse(addresses.isEmpty());
    List<InetSocketAddress> rpcAddrs = addresses.get("ozone1");
    assertFalse(rpcAddrs.isEmpty());
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.1")));
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.2")));
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.3")));
  }
}

