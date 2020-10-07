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

package org.apache.hadoop.hdds.upgrade;


import java.io.IOException;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureCatalog.HDDSLayoutFeature;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage layout versions and features for Storage Container Manager
 * and DataNodes.
 */
@SuppressWarnings("FinalClass")
public class HDDSLayoutVersionManager extends
    AbstractLayoutVersionManager<HDDSLayoutFeature> {

  private static HDDSLayoutVersionManager hddsLayoutVersionManager;

  private HDDSLayoutVersionManager() {
  }

  /**
   * Read only instance to HDDS Version Manager.
   * @return version manager instance.
   */
  public static synchronized LayoutVersionManager getInstance() {
    if (hddsLayoutVersionManager == null) {
      throw new RuntimeException("HDDS Layout Version Manager not yet " +
          "initialized.");
    }
    return hddsLayoutVersionManager;
  }


  /**
   * Initialize HDDS version manager from scmstorage.
   * @return version manager instance.
   */
  public static synchronized HDDSLayoutVersionManager initialize(
      Storage hddsStorage)
      throws IOException {
    if (hddsLayoutVersionManager == null) {
      hddsLayoutVersionManager = new HDDSLayoutVersionManager();
      hddsLayoutVersionManager.init(hddsStorage.getLayoutVersion(),
          HDDSLayoutFeature.values());
    }
    return hddsLayoutVersionManager;
  }

  @VisibleForTesting
  protected synchronized static void resetLayoutVersionManager() {
    if (hddsLayoutVersionManager != null) {
      hddsLayoutVersionManager.reset();
      hddsLayoutVersionManager = null;
    }
  }
}
