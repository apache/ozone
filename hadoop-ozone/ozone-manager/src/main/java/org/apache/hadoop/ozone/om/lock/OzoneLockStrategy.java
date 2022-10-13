/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.ozone.om.OMMetadataManager;

import java.io.IOException;

/**
 * This is a common strategy interface for all concrete lock strategies. The
 * interface declares methods (for acquiring/releasing a read/write lock) the
 * context (here OzoneManager class) uses to execute a lock strategy. The
 * concrete lock strategies (which implement different variations of the
 * methods defined in the interface) allow us in changing the behavior of the
 * locking mechanism at runtime.
 */
public interface OzoneLockStrategy {
  boolean acquireWriteLock(OMMetadataManager omMetadataManager,
                           String volumeName, String bucketName, String keyName)
      throws IOException;

  void releaseWriteLock(OMMetadataManager omMetadataManager, String volumeName,
                        String bucketName, String keyName);

  boolean acquireReadLock(OMMetadataManager omMetadataManager,
                          String volumeName, String bucketName, String keyName)
      throws IOException;

  void releaseReadLock(OMMetadataManager omMetadataManager, String volumeName,
                       String bucketName, String keyName);
}
