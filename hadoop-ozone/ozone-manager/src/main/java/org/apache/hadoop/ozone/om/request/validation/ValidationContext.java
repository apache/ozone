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

package org.apache.hadoop.ozone.om.request.validation;

import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManagerUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;

/**
 * A context that contains useful information for request validator instances.
 */
@InterfaceStability.Evolving
public interface ValidationContext {

  /**
   * Gets the {@link LayoutVersionManager} of the service, so that a pre
   * finalization validation can check if the layout version it belongs to
   * is finalized already or not.
   *
   * @return the {@link LayoutVersionManager} of the service
   */
  LayoutVersionManager versionManager();

  /**
   * Gets the {@link BucketLayout} of the given bucket. In case of a link bucket
   * the method returns the layout of the source bucket.
   *
   * @return {@link BucketLayout} of the given bucket.
   */
  BucketLayout getBucketLayout(String volumeName, String bucketName)
      throws IOException;

  /**
   * Creates a context object based on the given parameters.
   *
   * @param versionManager the {@link LayoutVersionManager} of the service
   * @return the {@link ValidationContext} specified by the parameters.
   */
  static ValidationContext of(LayoutVersionManager versionManager,
                              OMMetadataManager omMetadataManager) {

    return new ValidationContext() {
      @Override
      public LayoutVersionManager versionManager() {
        return versionManager;
      }

      @Override
      public BucketLayout getBucketLayout(String volumeName, String bucketName)
          throws IOException {
        return OzoneManagerUtils.getBucketLayout(omMetadataManager, volumeName,
            bucketName);
      }
    };
  }
}
