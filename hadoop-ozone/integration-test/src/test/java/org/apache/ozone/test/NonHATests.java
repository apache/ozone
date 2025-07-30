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

import org.apache.hadoop.fs.ozone.TestOzoneFSBucketLayout;
import org.apache.hadoop.fs.ozone.TestOzoneFSInputStream;
import org.apache.hadoop.fs.ozone.TestOzoneFSWithObjectStoreCreate;
import org.apache.hadoop.fs.ozone.TestOzoneFileSystemMetrics;
import org.apache.hadoop.fs.ozone.TestOzoneFileSystemMissingParent;
import org.apache.hadoop.hdds.scm.TestAllocateContainer;
import org.apache.hadoop.hdds.scm.TestContainerOperations;
import org.apache.hadoop.hdds.scm.TestContainerReportWithKeys;
import org.apache.hadoop.hdds.scm.TestContainerSmallFile;
import org.apache.hadoop.hdds.scm.TestGetCommittedBlockLengthAndPutKey;
import org.apache.hadoop.hdds.scm.TestSCMMXBean;
import org.apache.hadoop.hdds.scm.TestSCMNodeManagerMXBean;
import org.apache.hadoop.hdds.scm.TestXceiverClientManager;
import org.apache.hadoop.hdds.scm.container.metrics.TestSCMContainerManagerMetrics;
import org.apache.hadoop.hdds.scm.pipeline.TestNode2PipelineMap;
import org.apache.hadoop.hdds.scm.pipeline.TestPipelineManagerMXBean;
import org.apache.hadoop.hdds.scm.pipeline.TestSCMPipelineMetrics;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestCpuMetrics;
import org.apache.hadoop.ozone.admin.om.lease.TestLeaseRecoverer;
import org.apache.hadoop.ozone.client.rpc.TestCloseContainerHandlingByClient;
import org.apache.hadoop.ozone.client.rpc.TestContainerStateMachineStream;
import org.apache.hadoop.ozone.client.rpc.TestDiscardPreallocatedBlocks;
import org.apache.hadoop.ozone.client.rpc.TestOzoneClientMultipartUploadWithFSO;
import org.apache.hadoop.ozone.client.rpc.TestOzoneRpcClientWithKeyLatestVersion;
import org.apache.hadoop.ozone.om.TestBucketLayoutWithOlderClient;
import org.apache.hadoop.ozone.om.TestListKeys;
import org.apache.hadoop.ozone.om.TestListKeysWithFSO;
import org.apache.hadoop.ozone.om.TestListStatus;
import org.apache.hadoop.ozone.om.TestObjectStore;
import org.apache.hadoop.ozone.om.TestObjectStoreWithFSO;
import org.apache.hadoop.ozone.om.TestObjectStoreWithLegacyFS;
import org.apache.hadoop.ozone.om.TestOmBlockVersioning;
import org.apache.hadoop.ozone.om.TestOzoneManagerListVolumes;
import org.apache.hadoop.ozone.om.TestOzoneManagerRestInterface;
import org.apache.hadoop.ozone.reconfig.TestDatanodeReconfiguration;
import org.apache.hadoop.ozone.reconfig.TestOmReconfiguration;
import org.apache.hadoop.ozone.reconfig.TestScmReconfiguration;
import org.apache.hadoop.ozone.shell.TestOzoneDebugReplicasVerify;
import org.apache.hadoop.ozone.shell.TestOzoneDebugShell;
import org.apache.hadoop.ozone.shell.TestReconfigShell;
import org.apache.hadoop.ozone.shell.TestReplicationConfigPreference;
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

  /** Test cases for non-HA cluster should implement this. */
  public interface TestCase {
    MiniOzoneCluster cluster();
  }

  @Nested
  class OzoneClientMultipartUploadWithFSO extends TestOzoneClientMultipartUploadWithFSO {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneFSBucketLayout extends TestOzoneFSBucketLayout {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneFSInputStream extends TestOzoneFSInputStream {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneFSWithObjectStoreCreate extends TestOzoneFSWithObjectStoreCreate {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneFileSystemMetrics extends TestOzoneFileSystemMetrics {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneFileSystemMissingParent extends TestOzoneFileSystemMissingParent {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class AllocateContainer extends TestAllocateContainer {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ContainerOperations extends TestContainerOperations {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ContainerReportWithKeys extends TestContainerReportWithKeys {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ContainerSmallFile extends TestContainerSmallFile {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class GetCommittedBlockLengthAndPutKey extends TestGetCommittedBlockLengthAndPutKey {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class SCMMXBean extends TestSCMMXBean {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class SCMNodeManagerMXBean extends TestSCMNodeManagerMXBean {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class XceiverClientManager extends TestXceiverClientManager {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class SCMContainerManagerMetrics extends TestSCMContainerManagerMetrics {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class Node2PipelineMap extends TestNode2PipelineMap {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class PipelineManagerMXBean extends TestPipelineManagerMXBean {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ReplicationConfigPreference extends TestReplicationConfigPreference {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class SCMPipelineMetrics extends TestSCMPipelineMetrics {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class CloseContainerHandlingByClient extends TestCloseContainerHandlingByClient {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ContainerStateMachineStream extends TestContainerStateMachineStream {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class CpuMetrics extends TestCpuMetrics {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class DiscardPreallocatedBlocks extends TestDiscardPreallocatedBlocks {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class LeaseRecoverer extends TestLeaseRecoverer {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class BucketLayoutWithOlderClient extends TestBucketLayoutWithOlderClient {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ListKeys extends TestListKeys {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ListKeysWithFSO extends TestListKeysWithFSO {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ListStatus extends TestListStatus {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ObjectStore extends TestObjectStore {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ObjectStoreWithFSO extends TestObjectStoreWithFSO {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ObjectStoreWithLegacyFS extends TestObjectStoreWithLegacyFS {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmBlockVersioning extends TestOmBlockVersioning {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneManagerListVolumes extends TestOzoneManagerListVolumes {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneManagerRestInterface extends TestOzoneManagerRestInterface {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneRpcClientWithKeyLatestVersion extends TestOzoneRpcClientWithKeyLatestVersion {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class DatanodeReconfiguration extends TestDatanodeReconfiguration {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmReconfiguration extends TestOmReconfiguration {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneDebugShell extends TestOzoneDebugShell {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ReconfigShell extends TestReconfigShell {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class ScmReconfiguration extends TestScmReconfiguration {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneDebugReplicasVerify extends TestOzoneDebugReplicasVerify {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }
}
