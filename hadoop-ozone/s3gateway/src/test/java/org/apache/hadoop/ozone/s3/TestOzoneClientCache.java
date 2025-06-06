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

package org.apache.hadoop.ozone.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.Test;

/**
 * Test class for @{@link OzoneClientCache}.
 */
public class TestOzoneClientCache {

  @Test
  public void testGetClientFailure() {
    OzoneConfiguration config = new OzoneConfiguration();
    config.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
    config.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "");
    OzoneClientCache subject = new OzoneClientCache(config);

    assertThrows(IOException.class, subject::initialize);
  }

  @Test
  public void testGetClientFailureWithMultipleServiceIds() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, "ozone1,ozone2");
    OzoneClientCache subject = new OzoneClientCache(configuration);

    IOException e = assertThrows(IOException.class, subject::initialize);
    assertThat(e.getMessage())
        .contains("More than 1 OzoneManager ServiceID");
  }

  @Test
  public void testGetClientFailureWithMultipleServiceIdsAndInternalServiceId() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, "ozone1");
    configuration.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, "ozone1,ozone2");
    OzoneClientCache subject = new OzoneClientCache(configuration);

    // Still test will fail, as config is not complete. But it should pass
    // the service id check.
    IOException e = assertThrows(IOException.class, subject::initialize);
    assertThat(e.getMessage())
        .doesNotContain("More than 1 OzoneManager ServiceID");
  }

}
