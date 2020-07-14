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

package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests Freon with MiniOzoneCluster and ChunkManagerDummyImpl.
 * Data validation is disabled in RandomKeyGenerator.
 */
public class TestDataValidateWithDummyContainers
    extends TestDataValidate {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataValidateWithDummyContainers.class);

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA, false);
    conf.setBoolean(OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
        false);
    startCluster(conf);
  }

  /**
   * Write validation is not supported for non-persistent containers.
   * This test is a no-op.
   */
  @Test
  @Override
  @SuppressWarnings("java:S2699") // no assertion since no-op
  public void validateWriteTest() {
    LOG.info("Skipping validateWriteTest for non-persistent containers.");
  }

  @AfterClass
  public static void shutdown() {
    shutdownCluster();
  }
}
