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
package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSCMHAConfiguration {
  private OzoneConfiguration conf;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
  }

  @Test
  public void testSetStorageDir() {
    SCMHAConfiguration scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    scmhaConfiguration.setRatisStorageDir("scm-ratis");
    conf.setFromObject(scmhaConfiguration);

    scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    Assert.assertEquals("scm-ratis", scmhaConfiguration.getRatisStorageDir());
  }

  @Test
  public void testRaftLogPurgeEnabled() {
    SCMHAConfiguration scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    scmhaConfiguration.setRaftLogPurgeEnabled(true);
    conf.setFromObject(scmhaConfiguration);

    scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    Assert.assertEquals(true, scmhaConfiguration.getRaftLogPurgeEnabled());
  }
}
