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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test SimpleContainerDownloader.
 */
public class TestSimpleContainerDownloader {

  @Test
  public void testGetContainerDataFromReplicasHappyPath() throws Exception {

    //GIVEN
    List<DatanodeDetails> datanodes = createDatanodes();

    SimpleContainerDownloader downloader =
        createDownloaderWithPredefinedFailures();

    //WHEN
    final Path result =
        downloader.getContainerDataFromReplicas(1L, datanodes)
            .get(1L, TimeUnit.SECONDS);

    //THEN
    Assert.assertEquals(datanodes.get(0).getUuidString(), result.toString());
  }

  @Test
  public void testGetContainerDataFromReplicasOneFailure() throws Exception {

    //GIVEN
    List<DatanodeDetails> datanodes = createDatanodes();

    SimpleContainerDownloader downloader =
        createDownloaderWithPredefinedFailures(datanodes.get(0));

    //WHEN
    final Path result =
        downloader.getContainerDataFromReplicas(1L, datanodes)
            .get(1L, TimeUnit.SECONDS);

    //THEN
    //first datanode is failed, second worked
    Assert.assertEquals(datanodes.get(1).getUuidString(), result.toString());
  }

  /**
   * Creates downloader which fails with datanodes in the arguments.
   */
  private SimpleContainerDownloader createDownloaderWithPredefinedFailures(
      DatanodeDetails... failedDatanodes
  ) {

    ConfigurationSource conf = new OzoneConfiguration();

    final List<DatanodeDetails> datanodes =
        Arrays.asList(failedDatanodes);

    return new SimpleContainerDownloader(conf, null) {

      @Override
      protected CompletableFuture<Path> downloadContainer(
          long containerId,
          DatanodeDetails datanode
      ) throws IOException {

        if (datanodes.contains(datanode)) {
          throw new IOException("Unavailable datanode");
        } else {

          //path includes the dn id to make it possible to assert.
          return CompletableFuture.completedFuture(
              Paths.get(datanode.getUuidString()));
        }

      }
    };
  }

  private List<DatanodeDetails> createDatanodes() {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    return datanodes;
  }
}