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
package org.apache.hadoop.ozone.s3;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for @{@link OzoneClientProducer}.
 */
public class TestOzoneClientProducer {

  private OzoneClientProducer producer;

  public TestOzoneClientProducer()
      throws Exception {
    producer = new OzoneClientProducer();
    OzoneConfiguration config = new OzoneConfiguration();
    config.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
    config.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "");
    producer.setOzoneConfiguration(config);
  }

  @Test
  public void testGetClientFailure() {
    try {
      producer.createClient();
      fail("testGetClientFailure");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }
  }

  @Test
  public void testGetClientFailureWithMultipleServiceIds() {
    try {
      OzoneConfiguration configuration = new OzoneConfiguration();
      configuration.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, "ozone1,ozone2");
      producer.setOzoneConfiguration(configuration);
      producer.createClient();
      fail("testGetClientFailureWithMultipleServiceIds");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
      Assert.assertTrue(ex.getMessage().contains(
          "More than 1 OzoneManager ServiceID"));
    }
  }

  @Test
  public void testGetClientFailureWithMultipleServiceIdsAndInternalServiceId() {
    try {
      OzoneConfiguration configuration = new OzoneConfiguration();
      configuration.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, "ozone1");
      configuration.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, "ozone1,ozone2");
      producer.setOzoneConfiguration(configuration);
      producer.createClient();
      fail("testGetClientFailureWithMultipleServiceIdsAndInternalServiceId");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
      // Still test will fail, as config is not complete. But it should pass
      // the service id check.
      Assert.assertFalse(ex.getMessage().contains(
          "More than 1 OzoneManager ServiceID"));
    }
  }

}
