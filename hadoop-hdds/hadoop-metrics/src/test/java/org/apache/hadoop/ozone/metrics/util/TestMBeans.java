/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.metrics.util;

import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test MXBean addition of key/value pairs to registered MBeans.
 */
public class TestMBeans implements DummyMXBean {

  private int counter = 1;

  @Test
  public void testRegister() throws Exception {
    ObjectName objectName = null;
    try {
      counter = 23;
      objectName = MBeans.register("UnitTest",
          "RegisterTest", this);

      MBeanServer platformMBeanServer =
          ManagementFactory.getPlatformMBeanServer();

      int jmxCounter = (int) platformMBeanServer
          .getAttribute(objectName, "Counter");
      assertEquals(counter, jmxCounter);
    } finally {
      if (objectName != null) {
        MBeans.unregister(objectName);
      }
    }
  }


  @Test
  public void testRegisterWithAdditionalProperties() throws Exception {
    ObjectName objectName = null;
    try {
      counter = 42;

      Map<String, String> properties = new HashMap<String, String>();
      properties.put("flavour", "server");
      objectName = org.apache.hadoop.metrics2.util.MBeans.register("UnitTest", "RegisterTest",
          properties, this);

      MBeanServer platformMBeanServer =
          ManagementFactory.getPlatformMBeanServer();
      int jmxCounter =
          (int) platformMBeanServer.getAttribute(objectName, "Counter");
      assertEquals(counter, jmxCounter);
    } finally {
      if (objectName != null) {
        org.apache.hadoop.metrics2.util.MBeans.unregister(objectName);
      }
    }
  }

  @Test
  public void testGetMbeanNameName() {
    HashMap<String, String> properties = new HashMap<>();

    ObjectName mBeanName = MBeans.getMBeanName("Service",
        "Name", properties);

    assertEquals("Service", MBeans.getMbeanNameService(mBeanName));

    properties.put("key", "value");
    mBeanName = MBeans.getMBeanName(
        "Service",
        "Name",
        properties);

    assertEquals("Service", MBeans.getMbeanNameService(mBeanName));

  }

  @Override
  public int getCounter() {
    return counter;
  }

}
