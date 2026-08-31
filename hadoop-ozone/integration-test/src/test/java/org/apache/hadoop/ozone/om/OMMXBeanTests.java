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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test cases for the JMX management interface for OM information.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class OMMXBeanTests implements NonHATests.TestCase {

  private OzoneManager om;
  private MBeanServer mbs;

  @BeforeAll
  void init() {
    om = cluster().getOzoneManager();
    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  @Test
  public void testOMMXBean() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=OzoneManager,"
            + "name=OzoneManagerInfo,"
            + "component=ServerRuntime");

    String namespace = (String) mbs.getAttribute(bean, "Namespace");
    assertEquals(om.getNamespace(), namespace);

    String rpcPort = (String) mbs.getAttribute(bean, "RpcPort");
    assertEquals(om.getRpcPort(), rpcPort);

    String hostname = (String) mbs.getAttribute(bean, "Hostname");
    assertEquals(om.getHostname(), hostname);

    String ratisLogDirectory = (String) mbs.getAttribute(bean, "RatisLogDirectory");
    assertEquals(om.getRatisLogDirectory(), ratisLogDirectory);

    String rocksDbDirectory = (String) mbs.getAttribute(bean, "RocksDbDirectory");
    assertEquals(om.getRocksDbDirectory(), rocksDbDirectory);

    Object ratisRoles = mbs.getAttribute(bean, "RatisRoles");
    assertNotNull(ratisRoles);
    assertEquals(om.getRatisRoles(), toRatisRolesList(ratisRoles));

    String ratisEvents = (String) mbs.getAttribute(bean, "RatisEvents");
    assertEquals(om.getRatisEvents(), ratisEvents);
  }

  private static List<List<String>> toRatisRolesList(Object ratisRoles) {
    String[][] ratisRolesArray = assertInstanceOf(String[][].class, ratisRoles);
    List<List<String>> ratisRolesList = new ArrayList<>();
    for (String[] row : ratisRolesArray) {
      ratisRolesList.add(Arrays.asList(row));
    }
    return ratisRolesList;
  }
}
