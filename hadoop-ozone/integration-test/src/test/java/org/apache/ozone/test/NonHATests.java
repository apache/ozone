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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * Group tests to be run with a single non-HA cluster.
 * <p/>
 * Specific tests are implemented in separate classes, and they are subclasses
 * here as {@link Nested} inner classes.  This allows running all tests in the
 * same cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class NonHATests extends ClusterForTests<MiniOzoneCluster> {

  /** Hook method for subclasses. */
  MiniOzoneCluster.Builder newClusterBuilder() {
    return MiniOzoneCluster.newBuilder(createOzoneConfig())
        .setNumDatanodes(5);
  }

  /** Test cases for non-HA cluster should implement this. */
  public interface TestCase {
    MiniOzoneCluster cluster();
  }

  @Nested
  class AllocateContainer extends org.apache.hadoop.hdds.scm.TestAllocateContainer {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ContainerReportWithKeys extends org.apache.hadoop.hdds.scm.TestContainerReportWithKeys {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ContainerSmallFile extends org.apache.hadoop.hdds.scm.TestContainerSmallFile {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class GetCommittedBlockLengthAndPutKey extends org.apache.hadoop.hdds.scm.TestGetCommittedBlockLengthAndPutKey {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class SCMMXBean extends org.apache.hadoop.hdds.scm.TestSCMMXBean {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class SCMNodeManagerMXBean extends org.apache.hadoop.hdds.scm.TestSCMNodeManagerMXBean {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class Node2PipelineMap extends org.apache.hadoop.hdds.scm.pipeline.TestNode2PipelineMap {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class PipelineManagerMXBean extends org.apache.hadoop.hdds.scm.pipeline.TestPipelineManagerMXBean {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class SCMPipelineMetrics extends org.apache.hadoop.hdds.scm.pipeline.TestSCMPipelineMetrics {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class CpuMetrics extends org.apache.hadoop.ozone.TestCpuMetrics {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class DiscardPreallocatedBlocks extends org.apache.hadoop.ozone.client.rpc.TestDiscardPreallocatedBlocks {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class DNRPCLoadGenerator extends org.apache.hadoop.ozone.freon.TestDNRPCLoadGenerator {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class LeaseRecoverer extends org.apache.hadoop.ozone.admin.om.lease.TestLeaseRecoverer {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ObjectStore extends org.apache.hadoop.ozone.om.TestObjectStore {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class BucketLayoutWithOlderClient extends org.apache.hadoop.ozone.om.TestBucketLayoutWithOlderClient {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ObjectStoreWithFSO extends org.apache.hadoop.ozone.om.TestObjectStoreWithFSO {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmBlockVersioning extends org.apache.hadoop.ozone.om.TestOmBlockVersioning {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneManagerRestInterface extends org.apache.hadoop.ozone.om.TestOzoneManagerRestInterface {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class DatanodeReconfiguration extends org.apache.hadoop.ozone.reconfig.TestDatanodeReconfiguration {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmReconfiguration extends org.apache.hadoop.ozone.reconfig.TestOmReconfiguration {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ScmReconfiguration extends org.apache.hadoop.ozone.reconfig.TestScmReconfiguration {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

}
