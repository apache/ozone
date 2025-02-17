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

package org.apache.hadoop.fs.ozone.contract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.cleanup;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractContractDeleteTest;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractContractLeaseRecoveryTest;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractContractUnbufferTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for Ozone contract tests.  Manages lifecycle of {@link MiniOzoneCluster}.
 * <p/>
 * All specific contract tests are implemented as {@link Nested} inner classes.  This allows
 * running all tests in the same cluster.
 * <p/>
 * Subclasses only need to implement {@link #createOzoneContract(Configuration)},
 * but can tweak configuration by also overriding {@link #createOzoneConfig()}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractOzoneContractTest extends ClusterForTests<MiniOzoneCluster> {

  private static final String CONTRACT_XML = "contract/ozone.xml";

  /**
   * This must be implemented by all subclasses.
   * @return the FS contract
   */
  abstract AbstractFSContract createOzoneContract(Configuration conf);

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = createBaseConfiguration();
    conf.addResource(CONTRACT_XML);
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return MiniOzoneCluster.newBuilder(createOzoneConfig())
        .setNumDatanodes(5)
        .build();
  }

  @Nested
  class TestContractCreate extends AbstractContractCreateTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractDistCp extends AbstractContractDistCpTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }

    @Override
    protected void deleteTestDirInTeardown() throws IOException {
      super.deleteTestDirInTeardown();
      cleanup("TEARDOWN", getLocalFS(), getLocalDir());
    }
  }

  @Nested
  class TestContractDelete extends AbstractContractDeleteTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractGetFileStatus extends AbstractContractGetFileStatusTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractMkdir extends AbstractContractMkdirTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractOpen extends AbstractContractOpenTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractRename extends AbstractContractRenameTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractRootDirectory extends AbstractContractRootDirectoryTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }

    @Override
    @Test
    public void testRmRootRecursive() throws Throwable {
      // OFS doesn't support creating files directly under root
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testRmRootRecursive();
    }

    @Override
    @Test
    public void testRmNonEmptyRootDirNonRecursive() throws Throwable {
      // OFS doesn't support creating files directly under root
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testRmNonEmptyRootDirNonRecursive();
    }

    @Override
    @Test
    public void testRmEmptyRootDirNonRecursive() throws Throwable {
      // Internally test deletes volume recursively
      // Which is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testRmEmptyRootDirNonRecursive();
    }

    @Override
    @Test
    public void testListEmptyRootDirectory() throws IOException {
      // Internally test deletes volume recursively
      // Which is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testListEmptyRootDirectory();
    }

    @Override
    @Test
    public void testSimpleRootListing() throws IOException {
      // Recursive list is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testSimpleRootListing();
    }

    @Override
    @Test
    public void testMkDirDepth1() throws Throwable {
      // Internally test deletes volume recursively
      // Which is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testMkDirDepth1();
    }
  }

  @Nested
  class TestContractSeek extends AbstractContractSeekTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractUnbuffer extends AbstractContractUnbufferTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  class TestContractLeaseRecovery extends AbstractContractLeaseRecoveryTest {

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }

    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    @Test
    public void testLeaseRecovery() throws Throwable {
      assumeThat(getContract().getConf().get(OZONE_DEFAULT_BUCKET_LAYOUT,
          BucketLayout.FILE_SYSTEM_OPTIMIZED.name()))
          .isEqualTo(BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
      super.testLeaseRecovery();
    }

    @Override
    @Test
    public void testLeaseRecoveryFileNotExist() throws Throwable {
      assumeThat(getContract().getConf().get(OZONE_DEFAULT_BUCKET_LAYOUT,
          BucketLayout.FILE_SYSTEM_OPTIMIZED.name()))
          .isEqualTo(BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
      super.testLeaseRecoveryFileNotExist();
    }

    @Override
    @Test
    public void testLeaseRecoveryFileOnDirectory() throws Throwable {
      assumeThat(getContract().getConf().get(OZONE_DEFAULT_BUCKET_LAYOUT,
          BucketLayout.FILE_SYSTEM_OPTIMIZED.name()))
          .isEqualTo(BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
      super.testLeaseRecoveryFileOnDirectory();
    }
  }

}
