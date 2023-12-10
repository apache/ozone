/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.net;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;

import static org.apache.hadoop.hdds.scm.net.NetConstants.DEFAULT_NODEGROUP;
import static org.apache.hadoop.hdds.scm.net.NetConstants.DEFAULT_RACK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the node schema loader. */
@Timeout(30)
public class TestNodeSchemaManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNodeSchemaManager.class);
  private ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();
  private NodeSchemaManager manager;
  private OzoneConfiguration conf;

  public TestNodeSchemaManager() {
    conf = new OzoneConfiguration();
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.xml").getPath();
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    manager = NodeSchemaManager.getInstance();
    manager.init(conf);
  }

  @Test
  public void testFailure1() {
    assertThrows(IllegalArgumentException.class,
        () -> manager.getCost(0));
  }

  @Test
  public void testFailure2() {
    assertThrows(IllegalArgumentException.class,
        () -> manager.getCost(manager.getMaxLevel() + 1));
  }

  @Test
  public void testPass() {
    assertEquals(4, manager.getMaxLevel());
    for (int i  = 1; i <= manager.getMaxLevel(); i++) {
      assertTrue(manager.getCost(i) == 1 || manager.getCost(i) == 0);
    }
  }

  @Test
  public void testInitFailure() {
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.xml").getPath() + ".backup";
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    Throwable e = assertThrows(RuntimeException.class,
        () -> manager.init(conf));
    assertTrue(e.getMessage().contains("Failed to load schema file:" +
        filePath));
  }

  @Test
  public void testComplete() {
    // successful complete action
    String path = "/node1";
    assertEquals(DEFAULT_RACK + DEFAULT_NODEGROUP + path,
        manager.complete(path));
    assertEquals("/rack" + DEFAULT_NODEGROUP + path,
        manager.complete("/rack" + path));
    assertEquals(DEFAULT_RACK + "/nodegroup" + path,
        manager.complete("/nodegroup" + path));

    // failed complete action
    assertNull(manager.complete("/dc" + path));
  }
}
