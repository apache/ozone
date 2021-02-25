/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractUnbufferTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

/**
 * Ozone contract tests for {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer}.
 */
@RunWith(Parameterized.class)
public class ITestOzoneContractUnbuffer extends AbstractContractUnbufferTest {

  private static boolean fsOptimizedServer;

  public ITestOzoneContractUnbuffer(boolean fsoServer)
      throws IOException {
    if (fsOptimizedServer != fsoServer) {
      setFsOptimizedServer(fsoServer);
      ITestOzoneContractUtils.restartCluster(
          fsOptimizedServer);
    }
  }

  public static void setFsOptimizedServer(boolean fsOptimizedServer) {
    ITestOzoneContractUnbuffer.fsOptimizedServer = fsOptimizedServer;
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    OzoneContract.destroyCluster();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new OzoneContract(conf);
  }

  @Parameterized.Parameters
  public static Collection data() {
    return ITestOzoneContractUtils.getFsoCombinations();
  }
}