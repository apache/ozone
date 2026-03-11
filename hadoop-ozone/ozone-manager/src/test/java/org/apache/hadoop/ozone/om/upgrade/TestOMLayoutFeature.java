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

package org.apache.hadoop.ozone.om.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.Test;

/**
 * Tests invariants for legacy OM layout feature versions.
 */
public class TestOMLayoutFeature {
  @Test
  public void testOMLayoutFeaturesHaveIncreasingLayoutVersion()
      throws Exception {
    OMLayoutFeature[] values = OMLayoutFeature.values();
    int currVersion = -1;
    OMLayoutFeature lastFeature = null;
    for (OMLayoutFeature lf : values) {
      assertEquals(currVersion + 1, lf.layoutVersion());
      currVersion = lf.layoutVersion();
      lastFeature = lf;
    }
    lastFeature.addAction(arg -> {
      String v = arg.getVersion();
    });

    OzoneManager omMock = mock(OzoneManager.class);
    lastFeature.action().get().execute(omMock);
    verify(omMock, times(1)).getVersion();
  }

  /**
   * All incompatible changes to OM should now be added to {@link OzoneManagerVersion}.
   */
  @Test
  public void testNoNewOMLayoutFeaturesAdded() {
    int numOMLayoutFeatures = OMLayoutFeature.values().length;
    OMLayoutFeature lastFeature = OMLayoutFeature.values()[numOMLayoutFeatures - 1];
    assertEquals(10, numOMLayoutFeatures);
    assertEquals(OMLayoutFeature.SNAPSHOT_DEFRAG, lastFeature);
    assertEquals(9, lastFeature.layoutVersion());
  }
}
