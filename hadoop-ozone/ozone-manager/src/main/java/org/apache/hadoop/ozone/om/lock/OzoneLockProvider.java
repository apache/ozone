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

package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;

/**
 * OzoneLockProvider class returns the appropriate lock strategy pattern
 * implementation based on the configuration flags passed.
 */
public class OzoneLockProvider {

  private boolean keyPathLockEnabled;
  private boolean enableFileSystemPaths;

  public OzoneLockProvider(boolean keyPathLockEnabled,
                           boolean enableFileSystemPaths) {
    this.keyPathLockEnabled = keyPathLockEnabled;
    this.enableFileSystemPaths = enableFileSystemPaths;
  }

  public OzoneLockStrategy createLockStrategy(BucketLayout bucketLayout) {

    // TODO: This can be extended to support FSO, LEGACY_FS in the future.
    if (keyPathLockEnabled) {
      if (bucketLayout == BucketLayout.OBJECT_STORE) {
        return new OBSKeyPathLockStrategy();
      } else if (!enableFileSystemPaths &&
          bucketLayout == BucketLayout.LEGACY) {
        // old pre-created bucket with enableFileSystemPaths = false.
        return new OBSKeyPathLockStrategy();
      }
    }

    return new RegularBucketLockStrategy();
  }
}
