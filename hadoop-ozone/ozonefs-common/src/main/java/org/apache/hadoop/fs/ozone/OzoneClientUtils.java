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
package org.apache.hadoop.fs.ozone;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.ReplicatedFileChecksumHelper;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;

/**
 * Shared Utilities for Ozone FS and related classes.
 */
public final class OzoneClientUtils {
  private OzoneClientUtils(){
    // Not used.
  }
  public static BucketLayout resolveLinkBucketLayout(OzoneBucket bucket,
                                                     ObjectStore objectStore,
                                                     Set<Pair<String,
                                                         String>> visited)
      throws IOException {
    if (bucket.isLink()) {
      if (!visited.add(Pair.of(bucket.getVolumeName(),
          bucket.getName()))) {
        throw new OMException("Detected loop in bucket links. Bucket name: " +
            bucket.getName() + ", Volume name: " + bucket.getVolumeName(),
            DETECTED_LOOP_IN_BUCKET_LINKS);
      }

      OzoneBucket sourceBucket =
          objectStore.getVolume(bucket.getSourceVolume())
              .getBucket(bucket.getSourceBucket());

      /** If the source bucket is again a link, we recursively resolve the
       * link bucket.
       *
       * For example:
       * buck-link1 -> buck-link2 -> buck-link3 -> buck-link1 -> buck-src
       * buck-src has the actual BucketLayout that will be used by the links.
       */
      if (sourceBucket.isLink()) {
        return resolveLinkBucketLayout(sourceBucket, objectStore, visited);
      }
    }
    return bucket.getBucketLayout();
  }

  /*public static FileChecksum getFileChecksumInternal(
      String src, long length, Options.ChecksumCombineMode combineMode)
      throws IOException {
    ReplicatedFileChecksumHelper maker =
        new ReplicatedFileChecksumHelper(src, length, combineMode
        this);
    maker.compute();

    return maker.getFileChecksum();
  }*/
}
