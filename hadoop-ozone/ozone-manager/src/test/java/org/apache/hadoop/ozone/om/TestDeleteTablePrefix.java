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

package org.apache.hadoop.ozone.om;

import mockit.Expectations;
import mockit.integration.junit4.JMockit;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test delete table prefix is correctly built.
 */
@RunWith(JMockit.class)
public class TestDeleteTablePrefix {
  @Test
  public void testKeyForDeleteTable() {
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setObjectID(42).build();
    Assert.assertEquals("0000000000000001-2A-0",
            new DeleteTablePrefix(1L, true).buildKey(omKeyInfo));

    long current = Time.now();
    String expectedTimestamp = String.format("%016X-%016X-2A-0", 3L, current);

    new Expectations(Time.class) {
      {
        Time.now(); result = current;
      }
    };
    Assert.assertEquals(expectedTimestamp,
            new DeleteTablePrefix(3L, false).buildKey(omKeyInfo));

    Assert.assertEquals("0000000000000003-2A-0",
            new DeleteTablePrefix(3L, true).buildKey(omKeyInfo));

    Assert.assertEquals("0000000000000144-2A-0",
            new DeleteTablePrefix(324L, true).buildKey(omKeyInfo));
  }
}
