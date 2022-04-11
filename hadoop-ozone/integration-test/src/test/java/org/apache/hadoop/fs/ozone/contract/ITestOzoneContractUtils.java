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
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for Ozone-contract tests.
 */
public final class ITestOzoneContractUtils {

  private ITestOzoneContractUtils(){}

  private static List<Object> fsoCombinations = Arrays.asList(new Object[] {
      // FSO configuration is a cluster level server side configuration.
      // If the cluster is configured with SIMPLE metadata layout,
      // non-FSO bucket will created.
      // If the cluster is configured with PREFIX metadata layout,
      // FSO bucket will be created.
      // Presently, OzoneClient checks bucketMetadata then invokes FSO or
      // non-FSO specific code and it makes no sense to add client side
      // configs now. Once the specific client API to set FSO or non-FSO
      // bucket is provided the contract test can be refactored to include
      // another parameter (fsoClient) which sets/unsets the client side
      // configs.
      true, // Server is configured with new layout (PREFIX)
      // and new buckets will be operated on
      false // Server is configured with old layout (SIMPLE)
      // and old buckets will be operated on
  });

  static List<Object> getFsoCombinations(){
    return fsoCombinations;
  }

  public static void restartCluster(boolean fsOptimizedServer)
      throws IOException {
    OzoneContract.destroyCluster();
    OzoneContract.initOzoneConfiguration(
        fsOptimizedServer);
    OzoneContract.createCluster();
  }
}
