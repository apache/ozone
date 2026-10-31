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

package org.apache.hadoop.ozone.recon.tasks;

import java.util.concurrent.Callable;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesMetrics;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task for collecting the snapshot-trapped vs purgeable deleted-bytes split
 * for a single bucket from OM, via {@link OzoneManagerServiceProvider#getBucketDeletedBytes}.
 */
public class BucketDeletedBytesCollectionTask implements Callable<BucketDeletedBytesMetrics> {

  private static final Logger LOG = LoggerFactory.getLogger(BucketDeletedBytesCollectionTask.class);

  private final String volumeName;
  private final String bucketName;
  private final OzoneManagerServiceProvider ozoneManagerServiceProvider;

  public BucketDeletedBytesCollectionTask(
      String volumeName,
      String bucketName,
      OzoneManagerServiceProvider ozoneManagerServiceProvider) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
  }

  @Override
  public BucketDeletedBytesMetrics call() {
    LOG.debug("Collecting deleted bytes for bucket {}/{}", volumeName, bucketName);
    try {
      BucketDeletedBytes deletedBytes =
          ozoneManagerServiceProvider.getBucketDeletedBytes(volumeName + "/" + bucketName);
      return new BucketDeletedBytesMetrics(volumeName, bucketName, deletedBytes);
    } catch (Exception e) {
      LOG.error("Failed to collect deleted bytes for bucket {}/{}", volumeName, bucketName, e);
      BucketDeletedBytes unknown = BucketDeletedBytes.newBuilder()
          .setSnapshotTrappedBytes(-1L)
          .setPurgeableBytes(-1L)
          .setSnapshotTrappedKeys(-1L)
          .setPurgeableKeys(-1L)
          .setSnapshotTrappedDirs(-1L)
          .setPurgeableDirs(-1L)
          .setPurgeableKeyBytes(-1)
          .setPurgeableDirBytes(-1)
          .build();
      return new BucketDeletedBytesMetrics(volumeName, bucketName, unknown);
    }
  }
}
