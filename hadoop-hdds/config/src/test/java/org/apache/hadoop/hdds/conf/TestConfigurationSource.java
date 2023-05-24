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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestConfigurationSource {

  @Test
  void getPropsMatchPrefixAndTrimPrefix() {
    MutableConfigurationSource c = new InMemoryConfiguration();
    c.set("somePrefix.key", "value");

    assertEquals(ImmutableMap.of("key", "value"),
        c.getPropsMatchPrefixAndTrimPrefix("somePrefix."));
  }

  @Test
  void getPropsMatchPrefix() {
    MutableConfigurationSource c = new InMemoryConfiguration();
    c.set("somePrefix.key", "value");

    assertEquals(ImmutableMap.of("somePrefix.key", "value"),
        c.getPropsMatchPrefix("somePrefix."));
  }
  @Test
  void reconfigurableProperties() {
    String prefix = "ozone.scm.client";
    ImmutableSet<String> expected = ImmutableSet.of(
        prefix + ".dynamic",
        prefix + ".grandpa.dyna"
    );

    ConfigurationExample obj = new InMemoryConfiguration().getObject(
        ConfigurationExample.class);

    assertEquals(expected, obj.reconfigurableProperties());
  }

  @Test
  void reconfiguration() {
    MutableConfigurationSource subject = new InMemoryConfiguration();
    ConfigurationExample orig = subject.getObject(ConfigurationExample.class);
    ConfigurationExample obj = subject.getObject(ConfigurationExample.class);

    subject.set("ozone.scm.client.dynamic", "updated");
    subject.setLong("ozone.scm.client.wait", orig.getWaitTime() + 42);
    subject.reconfigure(ConfigurationExample.class, obj);

    assertEquals("updated", obj.getDynamic());
    assertEquals(orig.getWaitTime(), obj.getWaitTime());
  }
}
