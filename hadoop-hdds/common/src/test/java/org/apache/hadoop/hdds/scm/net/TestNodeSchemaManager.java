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

package org.apache.hadoop.hdds.scm.net;

import static org.apache.hadoop.hdds.scm.net.NetConstants.DEFAULT_NODEGROUP;
import static org.apache.hadoop.hdds.scm.net.NetConstants.DEFAULT_RACK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.junit.jupiter.api.Test;

/** Test the node schema loader. */
class TestNodeSchemaManager {
  private final ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();
  private final NodeSchemaManager manager;
  private final OzoneConfiguration conf;

  TestNodeSchemaManager() {
    conf = new OzoneConfiguration();
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.xml").getPath();
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    manager = NodeSchemaManager.getInstance();
    manager.init(conf);
  }

  @Test
  void testFailure1() {
    assertThrows(IllegalArgumentException.class,
        () -> manager.getCost(0));
  }

  @Test
  void testFailure2() {
    assertThrows(IllegalArgumentException.class,
        () -> manager.getCost(manager.getMaxLevel() + 1));
  }

  @Test
  void testPass() {
    assertEquals(4, manager.getMaxLevel());
    for (int i  = 1; i <= manager.getMaxLevel(); i++) {
      assertThat(manager.getCost(i)).isIn(0, 1);
    }
  }

  @Test
  void testInitFailure() {
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.xml").getPath() + ".backup";
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    Throwable e = assertThrows(RuntimeException.class,
        () -> manager.init(conf));
    assertThat(e).hasMessageContaining("Failed to load schema file:" + filePath);
  }

  @Test
  void testComplete() {
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
