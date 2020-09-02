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
package org.apache.hadoop.hdds.conf;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * Test the configuration reflection utility class.
 */
public class TestConfigurationReflectionUtil {

  @Test
  public void testClassWithConfigGroup() {
    Optional<ConfigType> actualType =
        ConfigurationReflectionUtil.getType(
            ConfigurationExample.class, "waitTime");
    Assert.assertTrue(actualType.isPresent());
    Assert.assertEquals(ConfigType.TIME, actualType.get());

    Optional<String> actualKey =
        ConfigurationReflectionUtil.getKey(
        ConfigurationExample.class, "waitTime");
    Assert.assertTrue(actualKey.isPresent());
    Assert.assertEquals("ozone.scm.client.wait", actualKey.get());

    Optional<String> actualDefaultValue =
        ConfigurationReflectionUtil.getDefaultValue(
            ConfigurationExample.class, "waitTime");
    Assert.assertTrue(actualDefaultValue.isPresent());
    Assert.assertEquals("30m", actualDefaultValue.get());
  }

  @Test
  public void testClassWithoutConfigGroup() {
    Optional<ConfigType> actualType =
        ConfigurationReflectionUtil.getType(
            ConfigurationExampleGrandParent.class, "number");
    Assert.assertTrue(actualType.isPresent());
    Assert.assertEquals(ConfigType.AUTO, actualType.get());

    Optional<String> actualKey =
        ConfigurationReflectionUtil.getKey(
            ConfigurationExampleGrandParent.class, "number");
    Assert.assertTrue(actualKey.isPresent());
    Assert.assertEquals("number", actualKey.get());

    Optional<String> actualDefaultValue =
        ConfigurationReflectionUtil.getDefaultValue(
            ConfigurationExampleGrandParent.class, "number");
    Assert.assertTrue(actualDefaultValue.isPresent());
    Assert.assertEquals("2", actualDefaultValue.get());
  }
}
