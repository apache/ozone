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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

/**
 * Test for Ozone Manager configuration.
 */
public class TestOmConf {

  @Test
  public void testConf() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final OzoneManagerRatisServerConfig ratisConf = conf.getObject(
        OzoneManagerRatisServerConfig.class);
    assertEquals(0, ratisConf.getLogAppenderWaitTimeMin(),
        "getLogAppenderWaitTimeMin");
    assertWaitTimeMin(TimeDuration.ZERO, conf);

    ratisConf.setLogAppenderWaitTimeMin(1);
    conf.setFromObject(ratisConf);
    assertWaitTimeMin(TimeDuration.ONE_MILLISECOND, conf);

  }

  static void assertWaitTimeMin(TimeDuration expected,
      OzoneConfiguration conf) {
    final RaftProperties p = OzoneManagerRatisServer.newRaftProperties(
        conf, 1000, "dummy/dir");
    final TimeDuration t = RaftServerConfigKeys.Log.Appender.waitTimeMin(p);
    assertEquals(expected, t,
        RaftServerConfigKeys.Log.Appender.WAIT_TIME_MIN_KEY);
  }
}
