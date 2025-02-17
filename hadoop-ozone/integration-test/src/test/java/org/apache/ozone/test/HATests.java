/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.test;

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

import java.util.UUID;

/**
 * Group tests to be run with a single HA cluster.
 * <p/>
 * Specific tests are implemented in separate classes, and they are subclasses
 * here as {@link Nested} inner classes.  This allows running all tests in the
 * same cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class HATests extends ClusterForTests<MiniOzoneHAClusterImpl> {

  /** Hook method for subclasses. */
  MiniOzoneHAClusterImpl.Builder newClusterBuilder() {
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
  class OzoneFsHAURLs extends org.apache.hadoop.fs.ozone.TestOzoneFsHAURLs {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class ScmApplyTransactionFailure extends org.apache.hadoop.hdds.scm.container.TestScmApplyTransactionFailure {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class GetClusterTreeInformation extends org.apache.hadoop.ozone.TestGetClusterTreeInformation {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class DatanodeQueueMetrics extends org.apache.hadoop.ozone.container.metrics.TestDatanodeQueueMetrics {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

  @Nested
  class ScmAdminHA extends org.apache.hadoop.ozone.shell.TestScmAdminHA {
    @Override
    public MiniOzoneHAClusterImpl cluster() {
      return getCluster();
    }
  }

}
