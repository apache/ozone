/*
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
package org.apache.hadoop.hdds.conf.guice;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationExample;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test configuration injection.
 */
public class TestConfigInjector {

  @Test
  public void injectConfig() {

    //GIVEN
    InMemoryConfiguration config = new InMemoryConfiguration();
    config.set("ozone.scm.client.address", "asd");

    final Injector injector = Guice.createInjector(
        new ConfigurationModule(config),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(TestService.class);
          }
        });

    //WHEN
    final ConfigurationExample instance =
        injector.getInstance(ConfigurationExample.class);

    //THEN
    Assert.assertEquals("asd", instance.getClientAddress());

  }


  /**
   * Example service.
   */
  @ConfigGroup(prefix = "ozone.scm.client")
  public static class TestService {
    @Config(key = "address", defaultValue = "localhost", description = "Client "
        + "addres (To test string injection).", tags = ConfigTag.MANAGEMENT)
    private String clientAddress;

    public String getClientAddress() {
      return clientAddress;
    }

    public void setClientAddress(String clientAddress) {
      this.clientAddress = clientAddress;
    }
  }
}