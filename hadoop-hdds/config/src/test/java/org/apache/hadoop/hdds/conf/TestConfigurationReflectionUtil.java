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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * Test the configuration reflection utility class.
 */
@RunWith(Parameterized.class)
public class TestConfigurationReflectionUtil {

  private final ConfigType type;
  private final String key;
  private final String defaultValue;
  private final Class testClass;
  private final String fieldName;
  private final boolean typePresent;
  private final boolean keyPresent;
  private final boolean defaultValuePresent;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {ConfigurationExample.class, "waitTime",
            ConfigType.TIME, true,
            "ozone.scm.client.wait", true,
            "30m", true},
        {ConfigurationExampleGrandParent.class, "number",
            ConfigType.AUTO, true,
            "number", true,
            "2", true},
        {ConfigurationExample.class, "secure",
            ConfigType.AUTO, true,
            "ozone.scm.client.secure", true,
            "true", true},
        {ConfigurationExample.class, "no-such-field",
            null, false,
            "", false,
            "", false},
        {ConfigFileAppender.class, "document",
            null, false,
            "", false,
            "", false},
        {ConfigurationExample.class, "threshold",
            ConfigType.DOUBLE, true,
            "ozone.scm.client.threshold", true,
            "10", true},
    });
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public TestConfigurationReflectionUtil(
      Class testClass, String fieldName,
      ConfigType type, boolean typePresent,
      String key, boolean keyPresent,
      String defaultValue, boolean defaultValuePresent) {
    this.testClass = testClass;
    this.fieldName = fieldName;
    this.typePresent = typePresent;
    this.type = type;
    this.key = key;
    this.keyPresent = keyPresent;
    this.defaultValue = defaultValue;
    this.defaultValuePresent = defaultValuePresent;
  }

  @Test
  public void testForGivenClasses() {
    Optional<ConfigType> actualType =
        ConfigurationReflectionUtil.getType(
            testClass, fieldName);
    Assert.assertEquals(typePresent, actualType.isPresent());
    if (typePresent) {
      Assert.assertEquals(type, actualType.get());
    }

    Optional<String> actualKey =
        ConfigurationReflectionUtil.getKey(
            testClass, fieldName);
    Assert.assertEquals(keyPresent, actualKey.isPresent());
    if (keyPresent) {
      Assert.assertEquals(key, actualKey.get());
    }
    Optional<String> actualDefaultValue =
        ConfigurationReflectionUtil.getDefaultValue(
            testClass, fieldName);
    Assert.assertEquals(defaultValuePresent, actualDefaultValue.isPresent());
    if (defaultValuePresent) {
      Assert.assertEquals(defaultValue, actualDefaultValue.get());
    }
  }
}
