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

package org.apache.hadoop.hdds.scm.upgrade;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;

/**
 * Utility class to help test SCM upgrade scenarios.
 */
public final class ScmUpgradeTestUtils {

  private ScmUpgradeTestUtils() {
    // Utility class.
  }

  /**
   * Constructs a mock ScmVersionManager for an SCM which may be pre-finalized.
   */
  public static ScmVersionManager mockVersionManager(ComponentVersion apparentVersion) {
    ScmVersionManager manager = mock(ScmVersionManager.class);
    when(manager.getApparentVersion()).thenReturn(apparentVersion);
    when(manager.getSoftwareVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(manager.isAllowed(any(ComponentVersion.class)))
        .thenAnswer(v -> v.getArgument(0, ComponentVersion.class).isSupportedBy(apparentVersion));
    when(manager.needsFinalization()).thenReturn(!HDDSVersion.SOFTWARE_VERSION.equals(apparentVersion));
    return manager;
  }

  /**
   * Constructs a mock ScmVersionManager for a finalized SCM.
   */
  public static ScmVersionManager mockVersionManager() {
    return mockVersionManager(HDDSVersion.SOFTWARE_VERSION);
  }
}
