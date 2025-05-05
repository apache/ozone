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

import static org.apache.commons.io.output.NullOutputStream.NULL_OUTPUT_STREAM;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.toTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;
import org.apache.ozone.test.SpyOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

/**
 * Test for {@link PushReplicator}.
 */
class TestPushReplicator {

  private OzoneConfiguration conf;

  @BeforeEach
  void setup() {
    conf = new OzoneConfiguration();
  }

  @ParameterizedTest
  @EnumSource
  void uploadCompletesNormally(CopyContainerCompression compression)
      throws IOException {
    // GIVEN
    compression.setOn(conf);
    long containerID = randomContainerID();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    Consumer<CompletableFuture<Void>> completion =
        fut -> fut.complete(null);
    SpyOutputStream output = new SpyOutputStream(NULL_OUTPUT_STREAM);
    ContainerReplicator subject = createSubject(containerID, target,
        output, completion, compression);
    ReplicationTask task = new ReplicationTask(toTarget(containerID, target),
        subject);

    // WHEN
    subject.replicate(task);

    // THEN
    assertEquals(Status.DONE, task.getStatus());
    output.assertClosedExactlyOnce();
  }

  @Test
  void uploadFailsWithException() throws IOException {
    // GIVEN
    long containerID = randomContainerID();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    SpyOutputStream output = new SpyOutputStream(NULL_OUTPUT_STREAM);
    Consumer<CompletableFuture<Void>> completion =
        fut -> fut.completeExceptionally(new Exception("testing"));
    ContainerReplicator subject = createSubject(containerID, target,
        output, completion, NO_COMPRESSION);
    ReplicationTask task = new ReplicationTask(toTarget(containerID, target),
        subject);

    // WHEN
    subject.replicate(task);

    // THEN
    assertEquals(Status.FAILED, task.getStatus());
    output.assertClosedExactlyOnce();
  }

  @Test
  void packFailsWithException() throws IOException {
    // GIVEN
    long containerID = randomContainerID();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    SpyOutputStream output = new SpyOutputStream(NULL_OUTPUT_STREAM);
    Consumer<CompletableFuture<Void>> completion = fut -> {
      throw new RuntimeException();
    };
    ContainerReplicator subject = createSubject(containerID, target,
        output, completion, NO_COMPRESSION);
    ReplicationTask task = new ReplicationTask(toTarget(containerID, target),
        subject);

    // WHEN
    subject.replicate(task);

    // THEN
    assertEquals(Status.FAILED, task.getStatus());
    output.assertClosedExactlyOnce();
  }

  private static long randomContainerID() {
    return ThreadLocalRandom.current().nextLong();
  }

  private ContainerReplicator createSubject(
      long containerID, DatanodeDetails target, OutputStream outputStream,
      Consumer<CompletableFuture<Void>> completion,
      CopyContainerCompression compression
  ) throws IOException {
    ContainerReplicationSource source = mock(ContainerReplicationSource.class);
    ContainerUploader uploader = mock(ContainerUploader.class);
    ArgumentCaptor<CompletableFuture<Void>> futureArgument =
        ArgumentCaptor.forClass(CompletableFuture.class);
    ArgumentCaptor<CopyContainerCompression> compressionArgument =
        ArgumentCaptor.forClass(CopyContainerCompression.class);

    when(
        uploader.startUpload(eq(containerID), eq(target),
            futureArgument.capture(), compressionArgument.capture()
        ))
        .thenReturn(outputStream);

    doAnswer(invocation -> {
      compressionArgument.getAllValues().forEach(
          c -> assertEquals(compression, c)
      );
      completion.accept(futureArgument.getValue());
      return null;
    })
        .when(source)
        .copyData(eq(containerID), any(), compressionArgument.capture());

    return new PushReplicator(conf, source, uploader);
  }

}
