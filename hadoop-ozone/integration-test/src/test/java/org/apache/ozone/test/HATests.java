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

package org.apache.ozone.test;

import java.util.UUID;
import org.apache.hadoop.fs.ozone.TestOzoneFsHAURLs;
import org.apache.hadoop.hdds.scm.TestStorageContainerManagerHAWithAllRunning;
import org.apache.hadoop.hdds.scm.container.TestScmApplyTransactionFailure;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.TestGetClusterTreeInformation;
import org.apache.hadoop.ozone.container.metrics.TestDatanodeQueueMetrics;
import org.apache.hadoop.ozone.shell.TestScmAdminHA;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * Group tests to be run with a single HA cluster.
 * <p/>
 * Specific tests are implemented in separate classes, and they are subclasses
 * here as {@link Nested} inner classes.  This allows running all tests in the
 * same cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class HATests extends ClusterForTests<MiniOzoneHAClusterImpl> {

  @Override
  protected MiniOzoneHAClusterImpl.Builder newClusterBuilder() {
    return MiniOzoneCluster.newHABuilder(createOzoneConfig())
        .setOMServiceId("om-" + UUID.randomUUID())
        .setNumOfOzoneManagers(3)
        .setSCMServiceId("scm-" + UUID.randomUUID())
        .setNumOfStorageContainerManagers(3);
  }

  /** Test cases which need HA cluster should implement this. */
  public interface TestCase {
    MiniOzoneHAClusterImpl cluster();
  }

  @Nested
  class OzoneFsHAURLs extends TestOzoneFsHAURLs {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class StorageContainerManagerHAWithAllRunning extends TestStorageContainerManagerHAWithAllRunning {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class ScmApplyTransactionFailure extends TestScmApplyTransactionFailure {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class GetClusterTreeInformation extends TestGetClusterTreeInformation {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class DatanodeQueueMetrics extends TestDatanodeQueueMetrics {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class ScmAdminHA extends TestScmAdminHA {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

}
