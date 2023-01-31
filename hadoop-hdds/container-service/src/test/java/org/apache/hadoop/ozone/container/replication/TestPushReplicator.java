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
package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;
import org.apache.ozone.test.SpyOutputStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.apache.commons.io.output.NullOutputStream.NULL_OUTPUT_STREAM;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.toTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link PushReplicator}.
 */
class TestPushReplicator {

  @Test
  void uploadCompletesNormally() throws IOException {
    // GIVEN
    long containerID = randomContainerID();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    Consumer<CompletableFuture<Void>> completion =
        fut -> fut.complete(null);
    SpyOutputStream output = new SpyOutputStream(NULL_OUTPUT_STREAM);
    ContainerReplicator subject = createSubject(containerID, target,
        output, completion);
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
        output, completion);
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
        output, completion);
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

  private static ContainerReplicator createSubject(
      long containerID, DatanodeDetails target, OutputStream outputStream,
      Consumer<CompletableFuture<Void>> completion) throws IOException {
    ContainerReplicationSource source = mock(ContainerReplicationSource.class);
    ContainerUploader uploader = mock(ContainerUploader.class);
    ArgumentCaptor<CompletableFuture<Void>> captor =
        ArgumentCaptor.forClass(CompletableFuture.class);

    when(uploader.startUpload(eq(containerID), eq(target), captor.capture()))
        .thenReturn(outputStream);

    doAnswer(invocation -> {
      completion.accept(captor.getValue());
      return null;
    })
        .when(source)
        .copyData(containerID, outputStream, NO_COMPRESSION.name());

    return new PushReplicator(source, uploader);
  }

}
