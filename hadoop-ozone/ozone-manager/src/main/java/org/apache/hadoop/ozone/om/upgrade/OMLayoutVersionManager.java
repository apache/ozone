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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage layout versions and features for Ozone Manager.
 */
public final class OMLayoutVersionManager extends AbstractLayoutVersionManager {

  private static OMLayoutVersionManager omVersionManager;
  private OmRequestFactory omRequestFactory;

  private OMLayoutVersionManager() {
  }

  /**
   * Read only instance to OM Version Manager.
   * @return version manager instance.
   */
  public static synchronized LayoutVersionManager getInstance() {
    if (omVersionManager == null) {
      throw new RuntimeException("OM Layout Version Manager not yet " +
          "initialized.");
    }
    return omVersionManager;
  }


  /**
   * Initialize OM version manager from storage.
   * @return version manager instance.
   */
  public static synchronized OMLayoutVersionManager initialize(
      OMStorage omStorage)
      throws OMException {
    if (omVersionManager == null) {
      omVersionManager = new OMLayoutVersionManager();
      omVersionManager.init(omStorage);
    }
    return omVersionManager;
  }

  /**
   * Initialize the OM Layout Features and current Layout Version.
   * @param storage to read the current layout version.
   * @throws OMException on error.
   */
  private void init(Storage storage) throws OMException {
    init(storage.getLayoutVersion(), OMLayoutFeature.values());
    if (metadataLayoutVersion > softwareLayoutVersion) {
      throw new OMException(
          String.format("Cannot initialize VersionManager. Metadata " +
                  "layout version (%d) > software layout version (%d)",
              metadataLayoutVersion, softwareLayoutVersion),
          NOT_SUPPORTED_OPERATION);
    }
    omRequestFactory = new OmRequestFactory(this);
  }

  @VisibleForTesting
  protected synchronized static void resetLayoutVersionManager() {
    if (omVersionManager != null) {
      omVersionManager.reset();
      omVersionManager = null;
    }
  }

  public OmRequestFactory getVersionFactory() {
    return omRequestFactory;
  }

}
