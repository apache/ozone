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

  private AbstractLayoutVersionManager versionManager =
      new MockVersionManager();

  @Before
  public void setUp() {
    versionManager.reset();
  }

  @Test
  public void testInit() {
    versionManager.init(1,
        getTestLayoutFeatures(2));
    assertEquals(2, versionManager.features.size());
    assertEquals(2, versionManager.featureMap.size());
    assertEquals(1, versionManager.getMetadataLayoutVersion());
    assertEquals(2, versionManager.getSoftwareLayoutVersion());
    assertTrue(versionManager.needsFinalization());
  }

  @Test
  public void testNeedsFinalization() {
    versionManager.init(2, getTestLayoutFeatures(2));
    assertFalse(versionManager.needsFinalization());
  }

  private LayoutFeature[] getTestLayoutFeatures(int num) {
    LayoutFeature[] lfs = new LayoutFeature[num];
    int k = 0;
    for (int i = 1; i <= num; i++) {
      int finalI = i;
      lfs[k++] = new LayoutFeature() {
        @Override
        public String name() {
          return "LF-" + finalI;
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
    return lfs;
  }

  static class MockVersionManager extends AbstractLayoutVersionManager {
  }
}