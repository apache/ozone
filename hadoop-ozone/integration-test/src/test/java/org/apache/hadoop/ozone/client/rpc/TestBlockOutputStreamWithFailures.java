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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.hdds.scm.client.HddsClientUtils.checkForException;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.BLOCK_SIZE;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.BUCKET;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.CHUNK_SIZE;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.FLUSH_SIZE;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.MAX_FLUSH_SIZE;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.VOLUME;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.createCluster;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.createKey;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.getKeyName;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.newClient;
import static org.apache.hadoop.ozone.client.rpc.TestBlockOutputStream.newClientConfig;
import static org.apache.hadoop.ozone.container.TestHelper.validateData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.RatisBlockOutputStream;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests failure detection and handling in BlockOutputStream Class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Flaky("HDDS-11849")
class TestBlockOutputStreamWithFailures {

  private MiniOzoneCluster cluster;

  @BeforeAll
  void init() throws Exception {
    cluster = createCluster(25);
  }

  @AfterAll
  void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static Stream<Arguments> clientParameters() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false)
    );
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  void testContainerClose(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      testWatchForCommitWithCloseContainerException(client);
      testWatchForCommitWithSingleNodeRatis(client);
      testWriteMoreThanMaxFlushSize(client);
      testExceptionDuringClose(client);
    }
  }

  private void testWatchForCommitWithCloseContainerException(OzoneClient client)
      throws Exception {
    String keyName = getKeyName();
    int dataLength = MAX_FLUSH_SIZE + CHUNK_SIZE;
    byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
    KeyOutputStream keyOutputStream;
    RatisBlockOutputStream blockOutputStream;
    try (OzoneOutputStream key = createKey(client, keyName)) {
      key.write(data1);

      keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

      assertEquals(1, keyOutputStream.getStreamEntries().size());
      blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
          keyOutputStream.getStreamEntries().get(0).getOutputStream());

      // we have just written data more than flush Size(2 chunks), at this time
      // buffer pool will have 4 buffers allocated worth of chunk size

      assertEquals(4, blockOutputStream.getBufferPool().getSize());
      // writtenDataLength as well flushedDataLength will be updated here
      assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

      assertEquals(MAX_FLUSH_SIZE, blockOutputStream.getTotalDataFlushedLength());

      // since data equals to maxBufferSize is written, this will be a blocking
      // call and hence will wait for atleast flushSize worth of data to get
      // ack'd by all servers right here
      assertThat(blockOutputStream.getTotalAckDataLength())
          .isGreaterThanOrEqualTo(FLUSH_SIZE);

      // watchForCommit will clean up atleast one entry from the map where each
      // entry corresponds to flushSize worth of data
      assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
          .isLessThanOrEqualTo(1);

      // This will flush the data and update the flush length and the map.
      key.flush();

      // flush is a sync call, all pending operations will complete
      // Since the data in the buffer is already flushed, flush here will have
      // no impact on the counters and data structures

      assertEquals(4, blockOutputStream.getBufferPool().getSize());
      assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

      assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
      // flush will make sure one more entry gets updated in the map
      assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
          .isLessThanOrEqualTo(2);

      XceiverClientRatis raftClient =
          (XceiverClientRatis) blockOutputStream.getXceiverClient();
      assertEquals(3, raftClient.getCommitInfoMap().size());
      // Close the containers on the Datanode and write more data
      TestHelper.waitForContainerClose(key, cluster);
      key.write(data1);

      // As a part of handling the exception, 4 failed writeChunks  will be
      // rewritten plus one partial chunk plus two putBlocks for flushSize
      // and one flush for partial chunk
      key.flush();
      assertEquals(2, keyOutputStream.getStreamEntries().size());
      assertInstanceOf(ContainerNotOpenException.class,
          checkForException(blockOutputStream.getIoException()));

      // Make sure the retryCount is reset after the exception is handled
      assertEquals(0, keyOutputStream.getRetryCount());
      // commitInfoMap will remain intact as there is no server failure
      assertEquals(3, raftClient.getCommitInfoMap().size());
      // now close the stream, It will update ack length after watchForCommit
    }
    // make sure the bufferPool is empty
    assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
    assertEquals(0, keyOutputStream.getStreamEntries().size());
    // Written the same data twice
    byte[] bytes = ArrayUtils.addAll(data1, data1);
    validateData(keyName, bytes, client.getObjectStore(), VOLUME, BUCKET);
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  void testWatchForCommitDatanodeFailure(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      String keyName = getKeyName();
      int dataLength = MAX_FLUSH_SIZE + CHUNK_SIZE;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);
        // since its hitting the full bufferCondition, it will call watchForCommit
        // and completes at least putBlock for first flushSize worth of data
        keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        assertEquals(1, keyOutputStream.getStreamEntries().size());
        blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
            keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have just written data more than flush Size(2 chunks), at this time
        // buffer pool will have 3 buffers allocated worth of chunk size

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        // since data written is still less than flushLength, flushLength will
        // still be 0.
        assertEquals(MAX_FLUSH_SIZE,
            blockOutputStream.getTotalDataFlushedLength());

        // since data equals to maxBufferSize is written, this will be a blocking
        // call and hence will wait for atleast flushSize worth of data to get
        // ack'd by all servers right here
        assertThat(blockOutputStream.getTotalAckDataLength())
            .isGreaterThanOrEqualTo(FLUSH_SIZE);

        // watchForCommit will clean up atleast flushSize worth of data buffer
        // where each entry corresponds to flushSize worth of data
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(2);

        // This will flush the data and update the flush length and the map.
        key.flush();

        // Since the data in the buffer is already flushed, flush here will have
        // no impact on the counters and data structures

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        //  flush will make sure one more entry gets updated in the map
        assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());

        XceiverClientRatis raftClient =
            (XceiverClientRatis) blockOutputStream.getXceiverClient();
        assertEquals(3, raftClient.getCommitInfoMap().size());
        Pipeline pipeline = raftClient.getPipeline();
        stopAndRemove(pipeline.getNodes().get(0));

        // again write data with more than max buffer limit. This will call
        // watchForCommit again. Since the commit will happen 2 way, the
        // commitInfoMap will get updated for servers which are alive
        key.write(data1);

        key.flush();

        assertEquals(2, keyOutputStream.getStreamEntries().size());
        // now close the stream, It will update ack length after watchForCommit
      }
      // Make sure the retryCount is reset after the exception is handled
      assertEquals(0, keyOutputStream.getRetryCount());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(0, keyOutputStream.getStreamEntries().size());
      // Written the same data twice
      byte[] bytes = ArrayUtils.addAll(data1, data1);
      validateData(keyName, bytes, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  void test2DatanodesFailure(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      String keyName = getKeyName();
      int dataLength = MAX_FLUSH_SIZE + CHUNK_SIZE;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);
        // since its hitting the full bufferCondition, it will call watchForCommit
        // and completes atleast putBlock for first flushSize worth of data
        keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
            keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have just written data more than flush Size(2 chunks), at this time
        // buffer pool will have 3 buffers allocated worth of chunk size

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(MAX_FLUSH_SIZE,
            blockOutputStream.getTotalDataFlushedLength());

        // since data equals to maxBufferSize is written, this will be a blocking
        // call and hence will wait for atleast flushSize worth of data to get
        // acked by all servers right here
        assertThat(blockOutputStream.getTotalAckDataLength())
            .isGreaterThanOrEqualTo(FLUSH_SIZE);

        // watchForCommit will clean up atleast one entry from the map where each
        // entry corresponds to flushSize worth of data
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(1);

        // This will flush the data and update the flush length and the map.
        key.flush();

        // Since the data in the buffer is already flushed, flush here will have
        // no impact on the counters and data structures

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        // flush will make sure one more entry gets updated in the map
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(2);

        XceiverClientRatis raftClient =
            (XceiverClientRatis) blockOutputStream.getXceiverClient();
        assertEquals(3, raftClient.getCommitInfoMap().size());
        Pipeline pipeline = raftClient.getPipeline();
        stopAndRemove(pipeline.getNodes().get(0));
        stopAndRemove(pipeline.getNodes().get(1));
        // again write data with more than max buffer limit. This will call
        // watchForCommit again. Since the commit will happen 2 way, the
        // commitInfoMap will get updated for servers which are alive

        // 4 writeChunks = maxFlushSize + 2 putBlocks  will be discarded here
        // once exception is hit
        key.write(data1);

        // As a part of handling the exception, 4 failed writeChunks  will be
        // rewritten plus one partial chunk plus two putBlocks for flushSize
        // and one flush for partial chunk
        key.flush();

        Throwable ioException = checkForException(
            blockOutputStream.getIoException());
        // Since, 2 datanodes went down,
        // a) if the pipeline gets destroyed quickly it will hit
        //    GroupMismatchException.
        // b) will hit close container exception if the container is closed
        //    but pipeline is still not destroyed.
        // c) will fail with RaftRetryFailureException if the leader election
        //    did not finish before the request retry count finishes.
        assertTrue(ioException instanceof RaftRetryFailureException
            || ioException instanceof GroupMismatchException
            || ioException instanceof ContainerNotOpenException);
        // Make sure the retryCount is reset after the exception is handled
        assertEquals(0, keyOutputStream.getRetryCount());
        // now close the stream, It will update ack length after watchForCommit

      }
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(0, keyOutputStream.getLocationInfoList().size());
      validateData(keyName, data1, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  private void testWriteMoreThanMaxFlushSize(OzoneClient client)
      throws Exception {
    String keyName = getKeyName();
    int dataLength = MAX_FLUSH_SIZE + CHUNK_SIZE;
    byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
    KeyOutputStream keyOutputStream;
    RatisBlockOutputStream blockOutputStream;
    try (OzoneOutputStream key = createKey(client, keyName)) {
      key.write(data1);

      keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

      assertEquals(1, keyOutputStream.getStreamEntries().size());
      blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
          keyOutputStream.getStreamEntries().get(0).getOutputStream());

      assertThat(blockOutputStream.getBufferPool().getSize())
          .isLessThanOrEqualTo(4);
      assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

      assertEquals(400, blockOutputStream.getTotalDataFlushedLength());

      // This will flush the data and update the flush length and the map.
      key.flush();

      assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());

      XceiverClientRatis raftClient =
          (XceiverClientRatis) blockOutputStream.getXceiverClient();
      assertEquals(3, raftClient.getCommitInfoMap().size());
      // Close the containers on the Datanode and write more data
      TestHelper.waitForContainerClose(key, cluster);
      key.write(data1);

      // As a part of handling the exception, 2 failed writeChunks  will be
      // rewritten plus 1 putBlocks for flush
      // and one flush for partial chunk
      key.flush();

      assertInstanceOf(ContainerNotOpenException.class,
          checkForException(blockOutputStream.getIoException()));
      // Make sure the retryCount is reset after the exception is handled
      assertEquals(0, keyOutputStream.getRetryCount());

      // commitInfoMap will remain intact as there is no server failure
      assertEquals(3, raftClient.getCommitInfoMap().size());
      // now close the stream, It will update ack length after watchForCommit
    }
    // make sure the bufferPool is empty
    assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
    assertEquals(0, keyOutputStream.getLocationInfoList().size());
    // Written the same data twice
    byte[] bytes = ArrayUtils.addAll(data1, data1);
    validateData(keyName, bytes, client.getObjectStore(), VOLUME, BUCKET);
  }

  private void testExceptionDuringClose(OzoneClient client) throws Exception {
    String keyName = getKeyName();
    int dataLength = 167;
    byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
    KeyOutputStream keyOutputStream;
    RatisBlockOutputStream blockOutputStream;
    try (OzoneOutputStream key = createKey(client, keyName)) {
      key.write(data1);

      keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

      assertEquals(1, keyOutputStream.getStreamEntries().size());
      blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
          keyOutputStream.getStreamEntries().get(0).getOutputStream());

      assertThat(blockOutputStream.getBufferPool().getSize())
          .isLessThanOrEqualTo(2);
      assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

      assertEquals(0, blockOutputStream.getTotalDataFlushedLength());

      assertEquals(0, blockOutputStream.getTotalAckDataLength());

      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      // This will flush the data and update the flush length and the map.
      key.flush();
      // Since the data in the buffer is already flushed, flush here will have
      // no impact on the counters and data structures

      assertThat(blockOutputStream.getBufferPool().getSize())
          .isLessThanOrEqualTo(2);
      assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

      assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
      // flush will make sure one more entry gets updated in the map
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());

      XceiverClientRatis raftClient =
          (XceiverClientRatis) blockOutputStream.getXceiverClient();
      assertEquals(3, raftClient.getCommitInfoMap().size());
      // Close the containers on the Datanode and write more data
      TestHelper.waitForContainerClose(key, cluster);
      key.write(data1);

      // commitInfoMap will remain intact as there is no server failure
      assertEquals(3, raftClient.getCommitInfoMap().size());
      // now close the stream, It will hit exception
    }

    assertInstanceOf(ContainerNotOpenException.class,
        checkForException(blockOutputStream.getIoException()));
    // Make sure the retryCount is reset after the exception is handled
    assertEquals(0, keyOutputStream.getRetryCount());
    // make sure the bufferPool is empty
    assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
    assertEquals(0, keyOutputStream.getStreamEntries().size());
    // Written the same data twice
    byte[] bytes = ArrayUtils.addAll(data1, data1);
    validateData(keyName, bytes, client.getObjectStore(), VOLUME, BUCKET);
  }

  private void testWatchForCommitWithSingleNodeRatis(OzoneClient client)
      throws Exception {
    String keyName = getKeyName();
    int dataLength = MAX_FLUSH_SIZE + CHUNK_SIZE;
    byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
    KeyOutputStream keyOutputStream;
    RatisBlockOutputStream blockOutputStream;
    try (OzoneOutputStream key = createKey(client, keyName, 0, ReplicationFactor.ONE)) {
      key.write(data1);

      keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

      assertEquals(1, keyOutputStream.getStreamEntries().size());
      blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
          keyOutputStream.getStreamEntries().get(0).getOutputStream());

      // we have just written data more than flush Size(2 chunks), at this time
      // buffer pool will have up to 4 buffers allocated worth of chunk size

      assertThat(blockOutputStream.getBufferPool().getSize())
          .isLessThanOrEqualTo(4);
      // writtenDataLength as well flushedDataLength will be updated here
      assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

      assertEquals(MAX_FLUSH_SIZE, blockOutputStream.getTotalDataFlushedLength());

      // since data equals to maxBufferSize is written, this will be a blocking
      // call and hence will wait for atleast flushSize worth of data to get
      // ack'd by all servers right here
      assertThat(blockOutputStream.getTotalAckDataLength())
          .isGreaterThanOrEqualTo(FLUSH_SIZE);

      // watchForCommit will clean up atleast one entry from the map where each
      // entry corresponds to flushSize worth of data
      assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
          .isLessThanOrEqualTo(1);

      // This will flush the data and update the flush length and the map.
      key.flush();

      // Since the data in the buffer is already flushed, flush here will have
      // no impact on the counters and data structures

      assertThat(blockOutputStream.getBufferPool().getSize())
          .isLessThanOrEqualTo(4);
      assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

      assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
      // flush will make sure one more entry gets updated in the map
      assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
          .isLessThanOrEqualTo(2);

      XceiverClientRatis raftClient =
          (XceiverClientRatis) blockOutputStream.getXceiverClient();
      assertEquals(1, raftClient.getCommitInfoMap().size());
      // Close the containers on the Datanode and write more data
      TestHelper.waitForContainerClose(key, cluster);
      // 4 writeChunks = maxFlushSize + 2 putBlocks  will be discarded here
      // once exception is hit
      key.write(data1);

      // As a part of handling the exception, 4 failed writeChunks  will be
      // rewritten plus one partial chunk plus two putBlocks for flushSize
      // and one flush for partial chunk
      key.flush();

      assertInstanceOf(ContainerNotOpenException.class,
          checkForException(blockOutputStream.getIoException()));
      // Make sure the retryCount is reset after the exception is handled
      assertEquals(0, keyOutputStream.getRetryCount());
      // commitInfoMap will remain intact as there is no server failure
      assertEquals(1, raftClient.getCommitInfoMap().size());
      assertEquals(2, keyOutputStream.getStreamEntries().size());
      // now close the stream, It will update ack length after watchForCommit
    }
    // make sure the bufferPool is empty
    assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
    assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
    assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
    assertEquals(0, keyOutputStream.getLocationInfoList().size());
    // Written the same data twice
    byte[] bytes = ArrayUtils.addAll(data1, data1);
    validateData(keyName, bytes, client.getObjectStore(), VOLUME, BUCKET);
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  void testDatanodeFailureWithSingleNode(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      String keyName = getKeyName();
      int dataLength = MAX_FLUSH_SIZE + CHUNK_SIZE;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      Pipeline pipeline;
      try (OzoneOutputStream key = createKey(client, keyName, 0, ReplicationFactor.ONE)) {
        key.write(data1);
        // since its hitting the full bufferCondition, it will call watchForCommit
        // and completes at least putBlock for first flushSize worth of data
        keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        assertEquals(1, keyOutputStream.getStreamEntries().size());
        blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
            keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have just written data more than flush Size(2 chunks), at this time
        // buffer pool will have 3 buffers allocated worth of chunk size

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(MAX_FLUSH_SIZE,
            blockOutputStream.getTotalDataFlushedLength());

        // since data equals to maxBufferSize is written, this will be a blocking
        // call and hence will wait for atleast flushSize worth of data to get
        // ack'd by all servers right here
        assertThat(blockOutputStream.getTotalAckDataLength())
            .isGreaterThanOrEqualTo(FLUSH_SIZE);

        // watchForCommit will clean up atleast flushSize worth of data buffer
        // where each entry corresponds to flushSize worth of data
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(2);

        // This will flush the data and update the flush length and the map.
        key.flush();

        // Since the data in the buffer is already flushed, flush here will have
        // no impact on the counters and data structures

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        //  flush will make sure one more entry gets updated in the map
        assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());

        XceiverClientRatis raftClient =
            (XceiverClientRatis) blockOutputStream.getXceiverClient();
        assertEquals(1, raftClient.getCommitInfoMap().size());
        pipeline = raftClient.getPipeline();
        cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));

        // again write data with more than max buffer limit. This will call
        // watchForCommit again. No write will happen in the current block and
        // data will be rewritten to the next block.

        key.write(data1);
        key.flush();

        assertInstanceOf(RaftRetryFailureException.class,
            checkForException(blockOutputStream.getIoException()));
        assertEquals(1, raftClient.getCommitInfoMap().size());
        // Make sure the retryCount is reset after the exception is handled
        assertEquals(0, keyOutputStream.getRetryCount());
        assertEquals(2, keyOutputStream.getStreamEntries().size());
        // now close the stream, It will update ack length after watchForCommit
      }
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(0, keyOutputStream.getStreamEntries().size());
      assertEquals(0, keyOutputStream.getLocationInfoList().size());
      // Written the same data twice
      byte[] bytes = ArrayUtils.addAll(data1, data1);
      cluster.restartHddsDatanode(pipeline.getNodes().get(0), true);
      validateData(keyName, bytes, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  void testDatanodeFailureWithPreAllocation(boolean flushDelay, boolean enablePiggybacking)
      throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      String keyName = getKeyName();
      int dataLength = MAX_FLUSH_SIZE + CHUNK_SIZE;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      Pipeline pipeline;
      try (OzoneOutputStream key = createKey(client, keyName, 3 * BLOCK_SIZE, ReplicationFactor.ONE)) {
        key.write(data1);
        // since its hitting the full bufferCondition, it will call watchForCommit
        // and completes at least putBlock for first flushSize worth of data
        keyOutputStream = assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        assertEquals(3, keyOutputStream.getStreamEntries().size());
        blockOutputStream = assertInstanceOf(RatisBlockOutputStream.class,
            keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have just written data more than flush Size(2 chunks), at this time
        // buffer pool will have 3 buffers allocated worth of chunk size

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(MAX_FLUSH_SIZE,
            blockOutputStream.getTotalDataFlushedLength());

        // since data equals to maxBufferSize is written, this will be a blocking
        // call and hence will wait for atleast flushSize worth of data to get
        // ack'd by all servers right here
        assertThat(blockOutputStream.getTotalAckDataLength())
            .isGreaterThanOrEqualTo(FLUSH_SIZE);

        // watchForCommit will clean up atleast flushSize worth of data buffer
        // where each entry corresponds to flushSize worth of data
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(2);

        // This will flush the data and update the flush length and
        // the map.
        key.flush();

        // Since the data in the buffer is already flushed, flush here will have
        // no impact on the counters and data structures

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        //  flush will make sure one more entry gets updated in the map
        assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());

        XceiverClientRatis raftClient =
            (XceiverClientRatis) blockOutputStream.getXceiverClient();
        assertEquals(1, raftClient.getCommitInfoMap().size());
        pipeline = raftClient.getPipeline();
        cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));

        // again write data with more than max buffer limit. This will call
        // watchForCommit again. No write will happen and

        key.write(data1);
        key.flush();

        assertInstanceOf(RaftRetryFailureException.class,
            checkForException(blockOutputStream.getIoException()));

        // Make sure the retryCount is reset after the exception is handled
        assertEquals(0, keyOutputStream.getRetryCount());
        assertEquals(1, raftClient.getCommitInfoMap().size());

        // now close the stream, It will update ack length after watchForCommit
      }

      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(0, keyOutputStream.getLocationInfoList().size());

      cluster.restartHddsDatanode(pipeline.getNodes().get(0), true);

      // Written the same data twice
      byte[] bytes = ArrayUtils.addAll(data1, data1);
      validateData(keyName, bytes, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  private void stopAndRemove(DatanodeDetails dn) throws IOException {
    HddsDatanodeService datanode = cluster.getHddsDatanodes().remove(cluster.getHddsDatanodeIndex(dn));
    datanode.stop();
    datanode.join();
  }

}
