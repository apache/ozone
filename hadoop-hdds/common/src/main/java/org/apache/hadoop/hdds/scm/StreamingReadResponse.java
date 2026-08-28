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

package org.apache.hadoop.hdds.scm;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;

/**
 * Streaming read response holding datanode details and
 * request observer to send read requests.
 */
public class StreamingReadResponse {

  private final Object readyLock = new Object();
  /** Set once the call has terminated; guarded by {@link #readyLock}. */
  private boolean terminated;
  /** The failure that terminated the call, or null if it completed normally; guarded by {@link #readyLock}. */
  private Throwable terminationCause;
  private final DatanodeDetails dn;
  private final ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver;
  private final String name;

  public StreamingReadResponse(DatanodeDetails dn,
      ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver) {
    this.dn = dn;
    this.requestObserver = requestObserver;

    final String s = dn.getID().toString();
    this.name = "dn" + s.substring(s.lastIndexOf('-')) + "_stream";
  }

  public DatanodeDetails getDatanodeDetails() {
    return dn;
  }

  public ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> getRequestObserver() {
    return requestObserver;
  }

  /** Registered as the request stream's {@code onReadyHandler}. */
  public void signalReady() {
    synchronized (readyLock) {
      readyLock.notifyAll();
    }
  }

  /**
   * Called when the call has terminated, so that {@link #awaitReady(long)} fails immediately instead of waiting
   * for the timeout: a terminated stream never becomes ready and gRPC no longer invokes the {@code onReadyHandler}.
   *
   * @param cause the error that terminated the call, or null if it completed normally.
   */
  public void signalTerminated(Throwable cause) {
    synchronized (readyLock) {
      terminated = true;
      terminationCause = cause;
      readyLock.notifyAll();
    }
  }

  /**
   * Wait until the request stream can accept another message.
   *
   * @return true if the stream is ready, false if the timeout expired first.
   * @throws IOException if the call terminated before the stream became ready.
   */
  public boolean awaitReady(long timeoutNanos) throws InterruptedException, IOException {
    final long deadlineNanos = System.nanoTime() + timeoutNanos;
    synchronized (readyLock) {
      while (!requestObserver.isReady()) {
        if (terminated) {
          throw new IOException("Stream " + name + " terminated while waiting for it to become ready",
              terminationCause);
        }
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          return false;
        }
        TimeUnit.NANOSECONDS.timedWait(readyLock, remainingNanos);
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return name;
  }
}
