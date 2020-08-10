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

package org.apache.hadoop.ozone.shell;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test ozone client creation.
 */
public class TestOzoneAddressClientCreation {

  @Test
  public void implicitNonHA() throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    address.createClient(new InMemoryConfiguration());
    Assert.assertTrue(address.simpleCreation);
  }

  @Test
  public void implicitHAOneServiceId()
      throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfiguration(OZONE_OM_SERVICE_IDS_KEY, "service1"));
    Assert.assertFalse(address.simpleCreation);
    Assert.assertEquals("service1", address.serviceId);
  }

  @Test(expected = OzoneClientException.class)
  public void implicitHaMultipleServiceId()
      throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfiguration(OZONE_OM_SERVICE_IDS_KEY,
            "service1,service2"));
  }

  @Test
  public void explicitNonHAHostPort() throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:9862/vol1/bucket1/key1");
    address.createClient(new InMemoryConfiguration());
    Assert.assertFalse(address.simpleCreation);
    Assert.assertEquals("om", address.host);
    Assert.assertEquals(9862, address.port);
  }

  @Test
  public void explicitHAHostPortWithServiceId()
      throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:9862/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfiguration(OZONE_OM_SERVICE_IDS_KEY, "service1"));
    Assert.assertFalse(address.simpleCreation);
    Assert.assertEquals("om", address.host);
    Assert.assertEquals(9862, address.port);
  }

  @Test
  public void explicitAHostPortWithServiceIds()
      throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:9862/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfiguration(OZONE_OM_SERVICE_IDS_KEY,
            "service1,service2"));
    Assert.assertFalse(address.simpleCreation);
    Assert.assertEquals("om", address.host);
    Assert.assertEquals(9862, address.port);
  }

  @Test
  public void explicitNonHAHost() throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om/vol1/bucket1/key1");
    address.createClient(
        new InMemoryConfiguration(OZONE_OM_SERVICE_IDS_KEY, "service1"));
    Assert.assertFalse(address.simpleCreation);
    Assert.assertEquals("om", address.host);
  }

  @Test
  public void explicitHAHostPort() throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("o3://om:1234/vol1/bucket1/key1");
    address.createClient(new InMemoryConfiguration());
    Assert.assertFalse(address.simpleCreation);
    Assert.assertEquals("om", address.host);
    Assert.assertEquals(1234, address.port);
  }

  @Test(expected = OzoneClientException.class)
  public void explicitWrongScheme() throws OzoneClientException, IOException {
    TestableOzoneAddress address =
        new TestableOzoneAddress("ssh://host/vol1/bucket1/key1");
    address.createClient(new InMemoryConfiguration());
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