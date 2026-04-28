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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;

/**
 * Mockito helpers for {@link OMVersionManager} in unit tests.
 */
public final class OMVersionManagerTestUtils {

  private OMVersionManagerTestUtils() {
  }

  /**
   * Mock with apparent and software version both {@link OzoneManagerVersion#SOFTWARE_VERSION},
   * {@link OMVersionManager#needsFinalization()} false, and {@link OMVersionManager#isAllowed(ComponentVersion)}
   * true for any argument.
   */
  public static OMVersionManager mockFinalizedOmVersionManager() {
    OMVersionManager ovm = mock(OMVersionManager.class);
    when(ovm.getApparentVersion()).thenReturn(OzoneManagerVersion.SOFTWARE_VERSION);
    when(ovm.getSoftwareVersion()).thenReturn(OzoneManagerVersion.SOFTWARE_VERSION);
    when(ovm.needsFinalization()).thenReturn(false);
    when(ovm.isAllowed(any(ComponentVersion.class))).thenReturn(true);
    return ovm;
  }
}
