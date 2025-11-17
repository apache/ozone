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

package org.apache.hadoop.ozone.client.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link OzoneKMSUtil}.
 * */
public class TestOzoneKMSUtil {
  private OzoneConfiguration config;

  @BeforeEach
  public void setUp() {
    config = new OzoneConfiguration();
    config.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
  }

  @Test
  public void getKeyProvider() {
    IOException ioe =
        assertThrows(IOException.class, () -> OzoneKMSUtil.getKeyProvider(config, null));
    assertEquals(ioe.getMessage(), "KMS serverProviderUri is " + "not configured.");
  }
}
