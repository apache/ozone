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

package org.apache.hadoop.hdds.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.HDDSVersion;
import org.junit.jupiter.api.Test;

/**
 * Tests invariants for legacy HDDS layout feature versions.
 */
public class TestHDDSLayoutFeature {
  @Test
  public void testHDDSLayoutFeaturesHaveIncreasingLayoutVersion() {
    HDDSLayoutFeature[] values = HDDSLayoutFeature.values();
    int currVersion = -1;
    for (HDDSLayoutFeature lf : values) {
      assertEquals(currVersion + 1, lf.layoutVersion());
      currVersion = lf.layoutVersion();
    }
  }

  /**
   * All incompatible changes to HDDS (SCM and Datanodes) should now be added to {@link HDDSVersion}.
   */
  @Test
  public void testNoNewHDDSLayoutFeaturesAdded() {
    int numHDDSLayoutFeatures = HDDSLayoutFeature.values().length;
    HDDSLayoutFeature lastFeature = HDDSLayoutFeature.values()[numHDDSLayoutFeatures - 1];
    assertEquals(11, numHDDSLayoutFeatures);
    assertEquals(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION, lastFeature);
    assertEquals(10, lastFeature.layoutVersion());
  }
}
