/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone.contract;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.hadoop.fs.contract.ContractTestUtils.cleanup;


/**
 * Contract test suite covering S3A integration with DistCp.
 * Uses the block output stream, buffered to disk. This is the
 * recommended output mechanism for DistCP due to its scalability.
 * This test suite runs the server in File System Optimized mode.
 * <p>
 * Note: It isn't possible to convert this into a parameterized test due to
 * unrelated failures occurring  while trying to handle directories with names
 * containing '[' and ']' characters.
 */
public class ITestOzoneContractDistCpWithFSO
    extends AbstractContractDistCpTest {

  @BeforeClass
  public static void createCluster() throws IOException {
    OzoneContract.createCluster(true);
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    OzoneContract.destroyCluster();
  }

  @Override
  protected OzoneContract createContract(Configuration conf) {
    return new OzoneContract(conf);
  }

  @Override
  protected void deleteTestDirInTeardown() throws IOException {
    super.deleteTestDirInTeardown();
    cleanup("TEARDOWN", getLocalFS(), getLocalDir());
  }
}
