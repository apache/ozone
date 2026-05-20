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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.LEGACY;

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.Nested;

abstract class SnapshotTests extends ClusterForTests<MiniOzoneCluster> {

  @Override
  protected void onClusterReady() throws Exception {
    // stop the deletion services so that keys can still be read
    getCluster().getOzoneManager().getKeyManager().stop();
  }

  @Nested
  class OmSnapshotFileSystemFso extends TestOmSnapshotFileSystem {
    OmSnapshotFileSystemFso() {
      super(FILE_SYSTEM_OPTIMIZED, false);
    }

    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmSnapshotFileSystemFsoWithLinkedBuckets extends TestOmSnapshotFileSystem {
    OmSnapshotFileSystemFsoWithLinkedBuckets() {
      super(FILE_SYSTEM_OPTIMIZED, true);
    }

    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmSnapshotFileSystemLegacy extends TestOmSnapshotFileSystem {
    OmSnapshotFileSystemLegacy() {
      super(LEGACY, false);
    }

    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmSnapshotFileSystemLegacyWithLinkedBuckets extends TestOmSnapshotFileSystem {
    OmSnapshotFileSystemLegacyWithLinkedBuckets() {
      super(LEGACY, true);
    }

    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }
}
