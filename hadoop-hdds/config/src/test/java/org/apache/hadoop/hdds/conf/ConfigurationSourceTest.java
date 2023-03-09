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

package org.apache.hadoop.hdds.conf;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Map;

class ConfigurationSourceTest {

  @Test
  void getPropsMatchPrefixAndTrimPrefix() {
    MutableConfigurationSource c = new InMemoryConfiguration();
    c.set("somePrefix.key", "value");
    ConfigurationSource config = c;
    Map<String, String> entry =
        config.getPropsMatchPrefixAndTrimPrefix("somePrefix.");
    Assert.assertEquals("key", entry.keySet().toArray()[0]);
    Assert.assertEquals("value", entry.values().toArray()[0]);
  }

  @Test
  void getPropsMatchPrefix() {
    MutableConfigurationSource c = new InMemoryConfiguration();
    c.set("somePrefix.key", "value");
    ConfigurationSource config = c;
    Map<String, String> entry =
        config.getPropsMatchPrefix("somePrefix.");
    Assert.assertEquals("somePrefix.key",
        entry.keySet().toArray()[0]);
    Assert.assertEquals("value", entry.values().toArray()[0]);
  }
}
