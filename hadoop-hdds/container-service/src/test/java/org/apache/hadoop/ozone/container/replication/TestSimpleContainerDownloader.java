/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.replication;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test container downloader.
 */
public class TestSimpleContainerDownloader {

  private static final String SUCCESS_PATH = "downloaded";

  /**
   * Test if different datanode is used for each download attempt.
   */
  @Test(timeout = 1000L)
  public void testRandomSelection()
      throws ExecutionException, InterruptedException {

    //GIVEN
    final List<DatanodeDetails> datanodes = createDatanodes();

    SimpleContainerDownloader downloader =
        new SimpleContainerDownloader(new OzoneConfiguration(), null) {

          @Override
          protected CompletableFuture<Path> downloadContainer(
              long containerId, DatanodeDetails datanode
          ) throws Exception {
            //download is always successful.
            return CompletableFuture
                .completedFuture(Paths.get(datanode.getUuidString()));
          }
        };

    //WHEN executed, THEN at least once the second datanode should be returned.
    for (int i = 0; i < 10000; i++) {
      Path path = downloader.getContainerDataFromReplicas(1L, datanodes).get();
      if (path.toString().equals(datanodes.get(1).getUuidString())) {
        return;
      }
    }

    //there is 1/2^10_000 chance for false positive, which is practically 0.
    Assert.fail(
        "Datanodes are selected 10000 times but second datanode was never "
            + "used.");
  }

  private List<DatanodeDetails> createDatanodes() {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    return datanodes;
  }
}