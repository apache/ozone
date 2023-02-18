/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.ozone.contract;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Ozone contract tests opening files.
 */
@RunWith(Parameterized.class)
public class ITestOzoneContractOpen extends AbstractContractOpenTest {

  public ITestOzoneContractOpen(boolean fso) {
    // Actual init done in initParam().
  }

  @Parameterized.BeforeParam
  public static void initParam(boolean fso) throws IOException {
    OzoneContract.createCluster(fso);
  }

  @Parameterized.AfterParam
  public static void teardownParam() throws IOException {
    OzoneContract.destroyCluster();
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
  public static Collection<Boolean> data() {
    return ITestOzoneContractUtils.getFsoCombinations();
  }
}
