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