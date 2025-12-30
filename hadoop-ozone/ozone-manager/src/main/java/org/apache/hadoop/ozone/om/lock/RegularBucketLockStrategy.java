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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;

/**
 * Implementation of OzoneLockStrategy interface. Concrete strategy for regular
 * BUCKET_LOCK.
 */
public class RegularBucketLockStrategy implements OzoneLockStrategy {

  @Override
  public OMLockDetails acquireWriteLock(OMMetadataManager omMetadataManager,
                                  String volumeName, String bucketName,
                                  String keyName) throws IOException {
    OMFileRequest.validateBucket(omMetadataManager, volumeName, bucketName);
    return omMetadataManager.getLock()
        .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName);
  }

  @Override
  public OMLockDetails releaseWriteLock(OMMetadataManager omMetadataManager,
                               String volumeName, String bucketName,
                               String keyName) {
    return omMetadataManager.getLock()
        .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName);
  }

  @Override
  public OMLockDetails acquireReadLock(OMMetadataManager omMetadataManager,
                                 String volumeName, String bucketName,
                                 String keyName) throws IOException {
    OMFileRequest.validateBucket(omMetadataManager, volumeName, bucketName);
    return omMetadataManager.getLock()
        .acquireReadLock(BUCKET_LOCK, volumeName, bucketName);
  }

  @Override
  public OMLockDetails releaseReadLock(OMMetadataManager omMetadataManager,
                              String volumeName, String bucketName,
                              String keyName) {
    return omMetadataManager.getLock()
        .releaseReadLock(BUCKET_LOCK, volumeName, bucketName);
  }
}
