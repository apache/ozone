/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * Test generic layout management init and APIs.
 */
public class TestAbstractLayoutVersionManager {

  @Before
  public void setUp() {
    AbstractLayoutVersionManager.reset();
  }

  @Test
  public void testInit() {
    AbstractLayoutVersionManager.init(1,
        getTestLayoutFeatures(2, 2));
    assertEquals(2, AbstractLayoutVersionManager.features.size());
    assertEquals(4, AbstractLayoutVersionManager.featureMap.size());
    assertEquals(1, AbstractLayoutVersionManager.getMetadataLayoutVersion());
    assertEquals(2, AbstractLayoutVersionManager.getSoftwareLayoutVersion());
    assertTrue(AbstractLayoutVersionManager.needsFinalization());
  }

  @Test
  public void testNeedsFinalization() {
    AbstractLayoutVersionManager.init(2, getTestLayoutFeatures(2, 1));
    assertFalse(AbstractLayoutVersionManager.needsFinalization());
  }

  private LayoutFeature[] getTestLayoutFeatures(int num, int numPerVersion) {
    LayoutFeature[] lfs = new LayoutFeature[num * numPerVersion];
    int k = 0;
    for (int i = 1; i <= num; i++) {
      for (int j = 1; j <= numPerVersion; j++) {
        int finalI = i;
        int finalJ = j;
        lfs[k++] = new LayoutFeature() {
          @Override
          public String name() {
            return "LF-" + finalI + "-" + finalJ;
          }

          @Override
          public int layoutVersion() {
            return finalI;
          }

          @Override
          public String description() {
            return null;
          }
        };
      }
    }
    return lfs;
  }
}