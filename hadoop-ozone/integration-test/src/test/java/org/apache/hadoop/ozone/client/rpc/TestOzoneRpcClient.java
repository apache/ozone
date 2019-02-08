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

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;


/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
public class TestOzoneRpcClient extends TestOzoneRpcClientAbstract {

  /**
   * Create a MiniOzoneCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    startCluster(conf);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    shutdownCluster();
  }

  @Test
  public void testGetS3Secret() throws IOException {
    //Creates a secret since it does not exist
    S3SecretValue firstAttempt = TestOzoneRpcClient.getStore()
        .getS3Secret("HADOOP/JOHNDOE");

    //Fetches the secret from db since it was created in previous step
    S3SecretValue secondAttempt = TestOzoneRpcClient.getStore()
        .getS3Secret("HADOOP/JOHNDOE");

    //secret fetched on both attempts must be same
    Assert.assertTrue(firstAttempt.getAwsSecret()
        .equals(secondAttempt.getAwsSecret()));

    //access key fetched on both attempts must be same
    Assert.assertTrue(firstAttempt.getAwsAccessKey()
        .equals(secondAttempt.getAwsAccessKey()));
  }
}
