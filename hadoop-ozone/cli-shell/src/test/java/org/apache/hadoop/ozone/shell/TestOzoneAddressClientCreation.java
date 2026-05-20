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

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.junit.jupiter.api.Test;

/**
 * Test ozone client creation.
 */
public class TestOzoneAddressClientCreation {

  @Test
  public void implicitNonHA() throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    address.createClient(new InMemoryConfigurationForTesting());
    assertTrue(address.simpleCreation);
  }

  @Test
  public void implicitHAOneServiceId()
      throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfigurationForTesting(OZONE_OM_SERVICE_IDS_KEY, "service1"));
    assertFalse(address.simpleCreation);
    assertEquals("service1", address.serviceId);
  }

  @Test
  public void implicitHaMultipleServiceId()
      throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    assertThrows(OzoneClientException.class, () ->
        address.createClient(new InMemoryConfigurationForTesting(OZONE_OM_SERVICE_IDS_KEY,
            "service1,service2")));
  }

  @Test
  public void implicitHaMultipleServiceIdWithDefaultServiceId()
      throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    InMemoryConfigurationForTesting conf = new InMemoryConfigurationForTesting(OZONE_OM_SERVICE_IDS_KEY,
        "service1,service2");
    conf.set(OZONE_OM_INTERNAL_SERVICE_ID, "service2");

    address.createClient(conf);
    assertFalse(address.simpleCreation);
    assertEquals("service2", address.serviceId);
  }

  @Test
  public void implicitHaMultipleServiceIdWithDefaultServiceIdForS3()
      throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SERVICE_IDS_KEY, "service1,service2");
    conf.set(OZONE_OM_INTERNAL_SERVICE_ID, "service2");

    address.createClientForS3Commands(conf, null);
    assertFalse(address.simpleCreation);
    assertEquals("service2", address.serviceId);
  }

  @Test
  public void explicitHaMultipleServiceId()
      throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://service1/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfigurationForTesting(OZONE_OM_SERVICE_IDS_KEY,
            "service1,service2"));
    assertFalse(address.simpleCreation);
    assertEquals("service1", address.serviceId);
  }

  @Test
  public void explicitNonHAHostPort() throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:9862/vol1/bucket1/key1");
    address.createClient(new InMemoryConfigurationForTesting());
    assertFalse(address.simpleCreation);
    assertEquals("om", address.host);
    assertEquals(9862, address.port);
  }

  @Test
  public void explicitHAHostPortWithServiceId()
      throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:9862/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfigurationForTesting(OZONE_OM_SERVICE_IDS_KEY, "service1"));
    assertFalse(address.simpleCreation);
    assertEquals("om", address.host);
    assertEquals(9862, address.port);
  }

  @Test
  public void explicitAHostPortWithServiceIds()
      throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:9862/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfigurationForTesting(OZONE_OM_SERVICE_IDS_KEY,
            "service1,service2"));
    assertFalse(address.simpleCreation);
    assertEquals("om", address.host);
    assertEquals(9862, address.port);
  }

  @Test
  public void explicitNonHAHost() throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfigurationForTesting(OZONE_OM_SERVICE_IDS_KEY, "service1"));
    assertFalse(address.simpleCreation);
    assertEquals("om", address.host);
  }

  @Test
  public void explicitHAHostPort() throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:1234/vol1/bucket1/key1");
    address.createClient(new InMemoryConfigurationForTesting());
    assertFalse(address.simpleCreation);
    assertEquals("om", address.host);
    assertEquals(1234, address.port);
  }

  @Test
  public void explicitWrongScheme() throws IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("ssh://host/vol1/bucket1/key1");
    assertThrows(OzoneClientException.class, () ->
        address.createClient(new InMemoryConfigurationForTesting()));
  }

  /**
   * OzoneAddress with modification to make it easier to test.
   */
  @SuppressWarnings("checkstyle")
  private static class TestableOzoneAddress extends OzoneAddress {

    private String host;
    private int port;
    private boolean simpleCreation;
    private String serviceId;

    TestableOzoneAddress(String address) throws OzoneClientException {
      super(address);
    }

    TestableOzoneAddress() throws OzoneClientException {
    }

    @Override
    protected OzoneClient createRpcClient(ConfigurationSource conf)
        throws IOException {
      simpleCreation = true;
      return null;
    }

    @Override
    protected OzoneClient createRpcClientFromHostPort(
        String hostParam, int portParam, MutableConfigurationSource conf
    ) throws IOException {
      this.host = hostParam;
      this.port = portParam;
      return null;
    }

    @Override
    protected OzoneClient createRpcClientFromServiceId(
        String serviceIdParam, MutableConfigurationSource conf
    ) throws IOException {
      this.serviceId = serviceIdParam;
      return null;
    }
  }

}
