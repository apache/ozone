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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test SimpleContainerDownloader.
 */
public class TestSimpleContainerDownloader {

  @TempDir
  private Path tempDir;

  @Test
  public void testGetContainerDataFromReplicasHappyPath() throws Exception {

    //GIVEN
    List<DatanodeDetails> datanodes = createDatanodes();
    TestingContainerDownloader downloader =
        TestingContainerDownloader.successful();

    //WHEN
    Path result = downloader.getContainerDataFromReplicas(1L, datanodes,
        tempDir, NO_COMPRESSION);

    //THEN
    assertEquals(datanodes.get(0).getUuidString(),
        result.toString());
    downloader.verifyAllClientsClosed();
  }

  @Test
  public void testGetContainerDataFromReplicasDirectFailure()
      throws Exception {

    //GIVEN
    List<DatanodeDetails> datanodes = createDatanodes();

    TestingContainerDownloader downloader =
        TestingContainerDownloader.immediateFailureFor(datanodes.get(0));

    //WHEN
    final Path result =
        downloader.getContainerDataFromReplicas(1L, datanodes,
            tempDir, NO_COMPRESSION);

    //THEN
    //first datanode is failed, second worked
    assertEquals(datanodes.get(1).getUuidString(),
        result.toString());
    downloader.verifyAllClientsClosed();
  }

  @Test
  public void testGetContainerDataFromReplicasAsyncFailure() throws Exception {

    //GIVEN
    List<DatanodeDetails> datanodes = createDatanodes();

    TestingContainerDownloader downloader =
        TestingContainerDownloader.delayedFailureFor(datanodes.get(0));

    //WHEN
    final Path result =
        downloader.getContainerDataFromReplicas(1L, datanodes,
            tempDir, NO_COMPRESSION);

    //THEN
    //first datanode is failed, second worked
    assertEquals(datanodes.get(1).getUuidString(),
        result.toString());
    downloader.verifyAllClientsClosed();
  }

  /**
   * Test if different datanode is used for each download attempt.
   */
  @Test
  public void testRandomSelection() throws Exception {

    //GIVEN
    final List<DatanodeDetails> datanodes = createDatanodes();

    TestingContainerDownloader downloader =
        TestingContainerDownloader.randomOrder();

    //WHEN executed, THEN at least once the second datanode should be
    //returned.
    for (int i = 0; i < 10000; i++) {
      Path path = downloader.getContainerDataFromReplicas(1L, datanodes,
          tempDir, NO_COMPRESSION);
      if (path.toString().equals(datanodes.get(1).getUuidString())) {
        return;
      }
    }

    //there is 1/3^10_000 chance for false positive, which is practically 0.
    fail(
        "Datanodes are selected 10000 times but second datanode was never "
            + "used.");
    downloader.verifyAllClientsClosed();
  }

  private List<DatanodeDetails> createDatanodes() {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    return datanodes;
  }

  private static final class TestingContainerDownloader
      extends SimpleContainerDownloader {

    private final List<DatanodeDetails> failedDatanodes;
    private final boolean disableShuffle;
    private final boolean directException;
    private final List<GrpcReplicationClient> clients = new LinkedList<>();

    private final AtomicReference<DatanodeDetails> datanodeRef =
        new AtomicReference<>();

    static TestingContainerDownloader randomOrder() {
      return new TestingContainerDownloader(false, false);
    }

    static TestingContainerDownloader successful() {
      return new TestingContainerDownloader(true, false);
    }

    static TestingContainerDownloader immediateFailureFor(
        DatanodeDetails... failedDatanodes) {
      return new TestingContainerDownloader(true, true, failedDatanodes);
    }

    static TestingContainerDownloader delayedFailureFor(
        DatanodeDetails... failedDatanodes) {
      return new TestingContainerDownloader(true, false, failedDatanodes);
    }

    /**
     * Creates downloader which fails with datanodes in the arguments.
     *
     * @param directException if false the exception will be wrapped in the
     *                        returning future.
     */
    private TestingContainerDownloader(
        boolean disableShuffle, boolean directException,
        DatanodeDetails... failedDatanodes) {
      super(new OzoneConfiguration(), null);
      this.disableShuffle = disableShuffle;
      this.directException = directException;
      this.failedDatanodes = Arrays.asList(failedDatanodes);
    }

    @Override
    protected List<DatanodeDetails> shuffleDatanodes(
        List<DatanodeDetails> sourceDatanodes
    ) {
      return disableShuffle ? sourceDatanodes //turn off randomization
          : super.shuffleDatanodes(sourceDatanodes);
    }

    @Override
    protected GrpcReplicationClient createReplicationClient(
        DatanodeDetails datanode, CopyContainerCompression compression) {
      datanodeRef.set(datanode);
      GrpcReplicationClient client = mock(GrpcReplicationClient.class);
      clients.add(client);
      return client;
    }

    @Override
    protected CompletableFuture<Path> downloadContainer(
        GrpcReplicationClient client,
        long containerId, Path downloadPath) {

      DatanodeDetails datanode = datanodeRef.get();
      assertNotNull(datanode);

      if (failedDatanodes.contains(datanode)) {
        if (directException) {
          throw new RuntimeException("Unavailable datanode");
        } else {
          return CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("Unavailable datanode");
          });
        }
      } else {

        //path includes the dn id to make it possible to assert.
        return CompletableFuture.completedFuture(
            Paths.get(datanode.getUuidString()));
      }

    }

    private void verifyAllClientsClosed() throws Exception {
      for (GrpcReplicationClient each : clients) {
        verify(each).close();
      }
    }
  }
}
