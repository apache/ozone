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

package org.apache.hadoop.ozone.freon;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/**
 * Tests Freon OzoneClientPositionedReader with MiniOzoneCluster.
 */
public abstract class TestOzoneClientPositionedReader implements NonHATests.TestCase {

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void testPositionedReadsBesideSequentialReader(int streams) throws Exception {
    String keyName = "key1";
    int reads = 100;
    OzoneBucket bucket;
    try (OzoneClient client = cluster().newClient()) {
      bucket = DataTestUtil.createVolumeAndBucket(client);
      DataTestUtil.createStringKey(bucket, keyName, 256 * 1024);
    }

    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    CommandLine cmd = new Freon().getCmd();
    int exitCode = cmd.execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "ocpr",
        "-v", bucket.getVolumeName(),
        "-b", bucket.getName(),
        "-k", keyName,
        "-t", "4",
        "-n", String.valueOf(reads),
        "--streams", String.valueOf(streams),
        "--clients", String.valueOf(streams),
        "--read-size", "4096",
        "--sequential-readers", "1",
        "--sequential-read-size", "65536");
    assertEquals(0, exitCode, "Freon ocpr command failed");

    BaseFreonGenerator subject = (BaseFreonGenerator) cmd.getParseResult().subcommand().commandSpec().userObject();
    assertEquals(reads, subject.getSuccessCount());
    assertEquals(0, subject.getFailureCount());
  }
}
